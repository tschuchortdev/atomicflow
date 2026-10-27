package atomicflow

import atomicflow.Cacheable.Simple.given
import doobie.free.{connection as fc}
import doobie.implicits.*
import test.PostgresWorkflowRuntimeSuite

import scala.concurrent.duration.*

/** The technical-failure boundary: exceptions escaping the runtime's
  * transactions are wrapped in [[WorkflowTechnicalException]] (never persisted
  * as workflow or step failures), a terminal failure surfaces to public callers
  * as the workflow's own exception (never as the runner-facing
  * [[WorkflowRunResult.Failed]] outcome), and a discarded instance's stale
  * events are removed with it.
  */
class TechnicalFailureSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  test("runTransaction wraps an unknown error as WorkflowTechnicalException, preserving the cause") {
    val rt = newRuntime
    val raw = new RuntimeException("raw infrastructure failure")
    val caught = intercept[WorkflowTechnicalException] {
      rt.runTransaction(fc.raiseError[Unit](raw))
    }
    assertEquals(caught.getCause, raw)
  }

  test("runTransaction passes the library's own exceptions through unwrapped") {
    val rt = newRuntime
    intercept[WorkflowNotFoundException] {
      rt.runTransaction(fc.raiseError[Unit](new WorkflowNotFoundException("no such instance")))
    }
    intercept[LeaseLostException] {
      rt.runTransaction(fc.raiseError[Unit](LeaseLostException(WorkflowInstanceId("wf", "k"))))
    }
  }

  test("a technical failure inside a step body aborts the run instead of persisting a step failure") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "tech-step") { _ =>
      Step.atLeastOnce[String]("boom") {
        throw new WorkflowTechnicalException(new RuntimeException("db down"))
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))
    val row = run(
      sql"""
        SELECT terminal_state FROM workflow_instances
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.query[Option[String]].unique
    )
    assertEquals(row, None, "a technical failure must not terminalize the instance")
    val failedStep = run(
      sql"""
        SELECT 1 FROM workflow_steps
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
          AND state_kind = 'Failed'
      """.query[Int].option
    )
    assertEquals(failedStep, None, "a technical failure must not be persisted as a step failure")
  }

  test("a technical failure inside a parallel branch aborts the whole run") {
    val rt = newRuntime
    def fine(using WorkflowContext): String     = "fine"
    def boom(using WorkflowContext): String     = throw new WorkflowTechnicalException(new RuntimeException("db down"))
    val wf = Workflow[String, String](id = "tech-branch") { _ =>
      Workflow.parallel(Seq[WorkflowContext ?=> String](fine, boom))
      ""
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))
    val row = run(
      sql"""
        SELECT terminal_state FROM workflow_instances
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.query[Option[String]].unique
    )
    assertEquals(row, None, "a technical failure in a branch must not terminalize the instance")
  }

  test("run and blockUntilCompleted return the decoded failure of a terminal-failed instance as the Failed outcome") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "tech-failed") { _ =>
      throw new RuntimeException("user failure")
    }
    val inst = rt.createWorkflowInstance(wf, "k", "in")
    // The first run returns the runner-facing Failed outcome.
    val WorkflowRunResult.Failed(first) = rt.runWorkflowInstance(wf, inst.id)
    assertEquals(first.getMessage, "user failure")
    assert(!first.isInstanceOf[WorkflowTechnicalException])
    // The instance is durably terminal; a later run decodes and returns the
    // same failure — never re-executing the body.
    val WorkflowRunResult.Failed(again) = rt.runWorkflowInstance(wf, inst.id)
    assertEquals(again.getMessage, "user failure")
    assert(!again.isInstanceOf[WorkflowTechnicalException])
    val WorkflowRunResult.Failed(blocked) = inst.blockUntilCompleted(5.seconds)(using rt)
    assertEquals(blocked.getMessage, "user failure")
    assert(!blocked.isInstanceOf[WorkflowTechnicalException])
  }

  test("an await filter's user exception propagates with its original type, never as technical") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val boom = new RuntimeException("filter says no")
    val wf = Workflow[String, String](id = "tech-filter") { _ =>
      Step.await[String]("w", Awaitable.SignalEvent(signal, filter = _ => throw boom))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    signal.send(id, "hi")(using rt)
    val WorkflowRunResult.Failed(caught) = rt.runWorkflowInstance(wf, id)
    // The throwable codec reconstitutes the failure into its portable form
    // (rendering the original class and message) — the same shape every decoded
    // failure takes. Crucially it is never a technical failure.
    assertEquals(caught.getMessage, boom.getMessage)
    assert(!caught.isInstanceOf[WorkflowTechnicalException])
    val row = run(
      sql"""
        SELECT terminal_state FROM workflow_instances
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.query[Option[String]].unique
    )
    assertEquals(row, Some("failed"), "a filter failure is a workflow-code failure: terminal FAILED")
  }

  test("a technical failure in one parallel branch does not interrupt a sibling step") {
    val rt = newRuntime
    val siblingStarted = new java.util.concurrent.CountDownLatch(1)
    val releaseSibling = new java.util.concurrent.CountDownLatch(1)
    val siblingFinished = new java.util.concurrent.atomic.AtomicBoolean(false)
    def slow(using WorkflowContext): String =
      Step.atLeastOnce[String]("slow") {
        siblingStarted.countDown()
        // Blocks interruptibly: if the sibling's technical failure escaped into
        // Ox, this would be interrupted and could persist a durable failure.
        assert(releaseSibling.await(5, java.util.concurrent.TimeUnit.SECONDS))
        siblingFinished.set(true)
        "slow-ok"
      }
    def broken(using WorkflowContext): String =
      throw new WorkflowTechnicalException(new RuntimeException("db down"))
    // Releases the sibling while the technical failure has already fired, so a
    // rethrow-into-Ox implementation would interrupt it mid-step.
    val releaser = new Thread(() => {
      siblingStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)
      Thread.sleep(200)
      releaseSibling.countDown()
    })
    releaser.setDaemon(true)
    releaser.start()
    val wf = Workflow[String, String](id = "tech-sibling") { _ =>
      Workflow.parallel(Seq[WorkflowContext ?=> String](slow, broken))
      ""
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))
    assert(siblingFinished.get(), "the sibling branch must run to completion")
    val badRows = run(
      sql"""
        SELECT COUNT(*) FROM workflow_steps
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
          AND state_kind IN ('Failed', 'RetryPending')
      """.query[Int].unique
    )
    assertEquals(badRows, 0, "no infrastructure-caused durable step failure may be persisted")
  }

  test("discard-existing removes the previous incarnation's events") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "discard-events") { in => in }
    val id = rt.createWorkflowInstance(wf, "k", "first").id
    signal.send(id, "hello")(using rt)
    val eventsBefore = run(
      sql"""
        SELECT COUNT(*) FROM workflow_events
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.query[Long].unique
    )
    assert(eventsBefore > 0, "the signal event must exist before the discard")

    val discarded = rt.createWorkflowInstanceDiscardExisting(wf, "k", "second")
    assertEquals(discarded, true)
    val eventsAfter = run(
      sql"""
        SELECT COUNT(*) FROM workflow_events
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.query[Long].unique
    )
    assertEquals(eventsAfter, 0L, "the re-created identity must start with an empty event log")
  }
}
