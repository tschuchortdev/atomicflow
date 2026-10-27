package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class StepGuaranteesSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  test("atMostOnce returns Some(value); replay returns Some(cached) without re-executing") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var observed: Option[String] = None
    val wf = Workflow[String, String](id = "mo-happy") { in =>
      observed = Step.atMostOnce[String]("step") {
        counter.incrementAndGet()
        s"value-$in"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    assertEquals(observed, Some("value-a"))

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "replay must not re-execute")
    assertEquals(observed, Some("value-a"), "replay returns the cached value")
  }

  test("atMostOnce: an unresolved Started row on replay returns None without re-executing") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var observed: Option[String] = None
    val wf = Workflow[String, String](id = "mo-crash") { in =>
      observed = Step.atMostOnce[String]("step") {
        counter.incrementAndGet()
        "value"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    assertEquals(observed, Some("value"))

    run(
      sql"""
        UPDATE workflow_steps SET state_kind = 'Started', state_payload = ''
        WHERE workflow_id = 'mo-crash' AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}' AND step_id = 'step' AND step_version = 0
      """.update.run
    )

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "atMostOnce must not re-execute on an unresolved Started row")
    assertEquals(observed, None, "a lost effect is assumed over a double execution")
  }

  test("atMostOnce: a changed ensureUnchanged input raises StepInputConflictException") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var date = "2024-01-01"
    var conflict: Option[String] = None
    val wf = Workflow[String, String](id = "mo-ensure") { in =>
      try {
        Step.atMostOnce[String]("step", ensureUnchanged = Seq("date" -> date)) {
          counter.incrementAndGet()
          "result"
        }
      } catch {
        case e: StepInputConflictException => conflict = Some(e.getMessage)
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1)

    date = "2024-02-01"
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "ensureUnchanged conflict must not re-execute")
    assert(conflict.isDefined)
  }

  test("atMostOnce: a changed invalidateOn input re-executes and caches the new result") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var market = "EU"
    val wf = Workflow[String, String](id = "mo-invalidate") { in =>
      Step.atMostOnce[String]("step", invalidateOn = Seq("market" -> market)) {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1)

    market = "US"
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2, "the new result is cached")
  }

  test("atMostOnce: invalidateAfter expiry re-executes the body") {
    val clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"))
    val rt = newRuntime(clock)
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "mo-ttl") { in =>
      Step.atMostOnce[String]("step", invalidateAfter = 10.seconds) {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "before expiry the result is cached")

    clock.advance(11.seconds)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2, "after expiry the body re-executes")
  }

  test("atMostOnce: failure throws the decoded failure; replay is identical without re-executing") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var caught: Option[String] = None
    val wf = Workflow[String, String](id = "mo-fail") { in =>
      try {
        Step.atMostOnce[String]("step") {
          counter.incrementAndGet()
          throw new RuntimeException(s"boom-$in")
        }
        ()
      } catch {
        case e: Throwable => caught = Some(e.getMessage)
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "x")
    assertEquals(counter.get(), 1)
    val firstMessage = caught.get
    assert(firstMessage.contains("boom-x"))

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "replay must not re-execute the failed step body")
    assertEquals(caught.get, firstMessage, "replay throws the identical decoded failure")
  }

  test("getExecutionState reads AtMostOnce rows via the shared state_kind") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "mo-state") { in =>
      state = Step.getExecutionState[String]("step")
      Step.atMostOnce[String]("step") {
        TestControlFlow.suspend()
        "never"
      }
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(state, StepExecutionState.Started)
  }

  test("atMostOnce rows use step_version 0 and step_kind Step") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "mo-kind") { in =>
      Step.atMostOnce[String]("step") {
        "value"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    val row = run(
      sql"""
        SELECT step_kind, step_version, state_kind FROM workflow_steps
        WHERE workflow_id = 'mo-kind' AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}' AND step_id = 'step'
      """.query[
          (String, Long, String)
        ].option
    )
    assertEquals(row, Some(("Step", 0L, "Succeeded")))
  }

  test("atMostOnce: no named inputs — instance id is the sole cache key") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "mo-instance-key") { in =>
      Step.atMostOnce[String]("step") {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k1", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k1"))
    assertEquals(counter.get(), 1, "same instance replays from cache")

    rt.createAndRun(wf, "k2", "a")
    assertEquals(counter.get(), 2, "a different instance re-executes")
  }

  test("a step key stored for a different step kind fails the run with StepKeyConflictException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "key-conflict") { in =>
      Step.atLeastOnce[String]("shared") { "value" }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    // Craft a foreign row at the step's key: an await result whose cache has
    // already expired, so the step's replay path would discard and re-execute.
    run(
      sql"""
        INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at)
        VALUES (${wf.id}, 'k', '{}', 'shared', 1, 'Await', 'Succeeded', 'go', '', '2020-01-01T00:00:00Z')
      """.update.run
    )

    // The run engine replays body-level failures through the throwable codec,
    // so the conflict surfaces as the decoded failure carrying the conflict message.
    val WorkflowRunResult.Failed(failure) = rt.runWorkflowInstance(wf, id)
    assert(failure.toString.contains("StepKeyConflictException"))
    assertEquals(
      run(
        sql"""
          SELECT step_kind FROM workflow_steps
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}' AND step_id = 'shared'
        """.query[String].option
      ),
      Some("Await"),
      "the foreign row is preserved, not deleted by the invalidation path"
    )
  }
}
