package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.Sha256Fingerprinter

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.experimental
import scala.concurrent.duration.*

@experimental
class StepFailureMatchingSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  test("a StepFailed catch by plain key fires on the failing run, on replay, and again after a reset re-executes the step") {
    val rt = newRuntime
    val aExecs = new AtomicInteger(0)
    var caughtBy = "none"
    var caughtMsg = ""
    val wf = Workflow[String, String](id = "sfm-basic") { in =>
      try {
        Step.atLeastOnce[String]("a") {
          aExecs.incrementAndGet()
          throw new RuntimeException("boom-a")
        }
        Step.atLeastOnce[String]("b") { "b-ok" }
      } catch {
        case StepFailed(stepId"a", msg) => caughtBy = "a"; caughtMsg = msg
        case StepFailed(stepId"b", msg) => caughtBy = "b"; caughtMsg = msg
      }
      Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(caughtBy, "a", "the plain-key pattern must catch step a's failure, not step b's")
    assert(caughtMsg.contains("boom-a"), s"unexpected message: $caughtMsg")
    assertEquals(aExecs.get(), 1)

    caughtBy = "none"
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(caughtBy, "a", "replay rethrows the persisted failure as StepFailed and the same branch fires")
    assertEquals(aExecs.get(), 1, "replay must not re-execute the failed step body")
    assert(caughtMsg.contains("boom-a"), s"unexpected message: $caughtMsg")

    rt.resetWorkflow[String, String](id, stepId"/a")(using wf)
    caughtBy = "none"
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(caughtBy, "a", "after reset the re-executed step fails again and the same branch fires")
    assertEquals(aExecs.get(), 2, "reset erased the Failed row, so the body re-executed")
    assert(caughtMsg.contains("boom-a"), s"unexpected message: $caughtMsg")
  }

  test("a StepFailed catch matches a scoped step by anchored path but not by a sibling scope's path") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sfm-scoped-anchored") { in =>
      try {
        Workflow.scoped("orders") {
          Step.atLeastOnce[String]("charge") {
            throw new RuntimeException("boom-charge")
          }
        }
        "unreachable"
      } catch {
        case StepFailed(stepId"shipping/charge", msg) => s"WRONG-shipping:$msg"
        case StepFailed(stepId"/orders/charge", msg)  => s"orders:$msg"
      }
    }
    val result = rt.createAndRun(wf, "k1", "in")
    val message = result match {
      case WorkflowRunResult.Result(v) => v
      case other                       => fail(s"unexpected result: $other")
    }
    assert(message.startsWith("orders"), s"the shipping/charge pattern must not match: $message")
    assert(message.contains("boom-charge"), s"unexpected message: $message")
  }

  test("a StepFailed catch also matches a scoped step by unanchored suffix path") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "sfm-scoped-unanchored") { in =>
      try {
        Workflow.scoped("orders") {
          Step.atLeastOnce[String]("charge") {
            throw new RuntimeException("boom-charge")
          }
        }
        "unreachable"
      } catch {
        case StepFailed(stepId"orders/charge", msg) => s"orders:$msg"
      }
    }
    val result = rt.createAndRun(wf, "k2", "in")
    val message = result match {
      case WorkflowRunResult.Result(v) => v
      case other                       => fail(s"unexpected result: $other")
    }
    assert(message.startsWith("orders:java.lang.RuntimeException: boom-charge"), s"unexpected: $message")
  }

  test("a StepFailed catch hole binds the fingerprinted scope segment of a Fingerprintable-scoped step") {
    val rt = newRuntime
    val someIntValue = 42
    // The outer literal scope keeps the pattern's leading segment; the inner
    // Fingerprintable scope is fingerprinted, exactly as Workflow.scoped does.
    val wf = Workflow[String, String](id = "sfm-fingerprint") { in =>
      val expectedFingerprint = Sha256Fingerprinter.fingerprint(someIntValue).toString
      try {
        Workflow.scoped("scoped") {
          Workflow.scoped(someIntValue) {
            Step.atLeastOnce[String]("charge") {
              throw new RuntimeException("boom-fingerprint")
            }
          }
        }
        "unreachable"
      } catch {
        case StepFailed(stepId"scoped/$x/charge", msg) => s"binding-equal=${x == expectedFingerprint}:msg=$msg"
      }
    }
    val result = rt.createAndRun(wf, "k", "in")
    val message = result match {
      case WorkflowRunResult.Result(v) => v
      case other                       => fail(s"unexpected result: $other")
    }
    assertEquals(message, "binding-equal=true:msg=java.lang.RuntimeException: boom-fingerprint")
  }

  test("a StepFailed catch matches a failing step inside a loop iteration, ignoring the iteration marker") {
    val rt = newRuntime
    val execs = new AtomicInteger(0)
    var caughtScope = Vector.empty[String]
    var caughtKey = ""
    val wf = Workflow[String, String](id = "sfm-loop") { in =>
      try {
        Workflow.loop[Int, String]("loopname", 0) { (state, loop) =>
          Step.atLeastOnce[String]("stepkey") {
            execs.incrementAndGet()
            if state >= 2 then throw new RuntimeException("boom-iter")
            s"ok-$state"
          }
          if state < 2 then state + 1 else loop.break("done")
        }
        "loop-finished"
      } catch {
        case StepFailed(theStep @ stepId"loopname/stepkey", msg) =>
          caughtScope = theStep.scope.segments
          caughtKey = theStep.key
          s"caught:$msg"
      }
    }
    val result = rt.createAndRun(wf, "k", "in")
    result match {
      case WorkflowRunResult.Result(v) =>
        assert(v.startsWith("caught:") && v.contains("boom-iter"), s"the loop-marker-independent pattern must catch: $v")
      case other =>
        fail(s"unexpected result: $other")
    }
    assertEquals(execs.get(), 3, "two succeeding iterations plus the failing third one")
    assertEquals(caughtScope, Vector("loopname@2"), "the concrete scope segment carries the iteration marker")
    assertEquals(caughtKey, "stepkey")
  }

  test("a StepFailed catch matches a failing step inside a firstToRun race branch (the branch occupies two scope segments)") {
    val rt = newRuntime
    val s2 = Signal[String]("s2")
    var caughtScope = Vector.empty[String]
    var caughtKey = ""
    val wf = Workflow[String, String](id = "sfm-race") { in =>
      try {
        val result = Step.firstToRunWithoutSuspension[Int]("raceid")(
          "loser" -> { () => Step.atLeastOnce[Int]("boom") { throw new RuntimeException("boom-race") } },
          "wait"  -> { () => Step.await[String]("wait", Awaitable.SignalEvent(s2)); 2 }
        )
        s"no-catch:$result"
      } catch {
        case StepFailed(theStep @ stepId"raceid/loser/boom", msg) =>
          caughtScope = theStep.scope.segments
          caughtKey = theStep.key
          s"caught:$msg"
      }
    }
    val result = rt.createAndRun(wf, "k", "in")
    val message = result match {
      case WorkflowRunResult.Result(v) => v
      case other                       => fail(s"unexpected result: $other")
    }
    assert(message.startsWith("caught:") && message.contains("boom-race"), s"unexpected: $message")
    assertEquals(caughtScope, Vector("raceid", "loser"), "a race branch occupies two scope segments")
    assertEquals(caughtKey, "boom")
  }

  test("a step re-reached on replay and after a reset rethrows StepFailed with the same step id") {
    val rt = newRuntime
    var observedIds = Vector.empty[StepId]
    val execs = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "sfm-reset-rethrow") { in =>
      try {
        Step.atLeastOnce[String]("flaky") {
          execs.incrementAndGet()
          throw new RuntimeException("boom-flaky")
        }
        "unreachable"
      } catch {
        case StepFailed(theStep @ stepId"flaky", msg) => observedIds = observedIds :+ theStep
      }
      Step.await[Unit]("wait", Awaitable.Timer(3600.seconds))
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(execs.get(), 1, "the replay must not re-execute the failed body")
    rt.resetWorkflow[String, String](id, stepId"/flaky")(using wf)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(execs.get(), 2, "only the reset re-executed the body")

    val expected = StepId("flaky")
    assertEquals(observedIds, Vector(expected, expected, expected), "every rethrow presents the same step id")
  }
}
