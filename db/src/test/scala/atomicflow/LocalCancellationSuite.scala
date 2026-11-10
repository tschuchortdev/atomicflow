package atomicflow

import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.util.concurrent.TimeUnit
import scala.annotation.experimental

/** Cooperative cancellation of losing `Step.firstCompletedWithoutSuspension`
  * branches: the race cancels them through the local cancellation flag on
  * their WorkflowContext and they stop at their next runtime checkpoint.
  * Lives in package `atomicflow` so tests can observe the package-private
  * cancellation state deterministically.
  */
@experimental
class LocalCancellationSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def storedStepIds(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[String] =
    run(
      sql"""
        SELECT step_id FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key
      """.query[String].to[Vector]
    )

  private def awaitCancellation(c: WorkflowContext): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (!c.isCancellationRequested && System.nanoTime() < deadline) Thread.sleep(1)
    require(c.isCancellationRequested, "cancellation was not observed before the deadline")
  }

  test("a losing branch stops at its next checkpoint without executing further steps") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "lc-stop") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "winner" -> { () => Step.atLeastOnce[Int]("winner-step") { 1 } },
        "loser" -> { () =>
          awaitCancellation(summon[WorkflowContext])
          Step.atLeastOnce[Int]("loser-step") { 2 }
          2
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assert(!storedStepIds(wf.id, "k").contains("loser-step"), "the losing branch must not execute a step after cancellation")
  }

  test("a branch that catches the cancellation cannot become the winner") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "lc-swallow") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("race")(
        "loser" -> { () =>
          awaitCancellation(summon[WorkflowContext])
          try Step.atLeastOnce[Int]("loser-step") { 99 }
          catch { case _: WorkflowCancelledException => 99 }
        },
        "winner" -> { () => Step.atLeastOnce[Int]("winner-step") { 1 } }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"),
      "the first completer wins even though the loser is declared first and returns a value")
    assert(!storedStepIds(wf.id, "k").contains("loser-step"))
  }

  test("a nested race inside a losing branch propagates the cancellation") {
    val rt = newRuntime
    val innerStarted = new java.util.concurrent.CountDownLatch(1)
    var innerRaceCancelled = false
    val wf = Workflow[String, String](id = "lc-nested") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("outer")(
        "winner" -> { () =>
          innerStarted.await(10, TimeUnit.SECONDS)
          Step.atLeastOnce[Int]("outer-winner") { 1 }
        },
        "loser" -> { () =>
          try {
            Step.firstCompletedWithoutSuspension[Int]("inner")(
              "i1" -> { () =>
                innerStarted.countDown()
                awaitCancellation(summon[WorkflowContext])
                Step.atLeastOnce[Int]("inner-one") { 10 }
              },
              "i2" -> { () =>
                awaitCancellation(summon[WorkflowContext])
                Step.atLeastOnce[Int]("inner-two") { 20 }
              }
            )
            2
          } catch {
            case e: WorkflowCancelledException =>
              innerRaceCancelled = true
              throw e
          }
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assert(innerRaceCancelled, "the inner race must propagate the cancellation to the losing branch")
    val ids = storedStepIds(wf.id, "k")
    assert(!ids.contains("inner"), "the nested race must not record a winner")
    assert(!ids.contains("inner-one") && !ids.contains("inner-two"),
      "the nested race's branches must not execute steps after cancellation")
  }

  test("a race inside Workflow.uncancellable does not deliver cancellation after its branches suspend") {
    val rt = newRuntime
    val sig = Signal[String]("never")
    val wf = Workflow[String, String](id = "lc-uncancellable-suspend") { _ =>
      Workflow.uncancellable {
        Step.firstCompletedWithoutSuspension[Int]("race")(
          "a" -> { () => Step.await[String]("a1", Awaitable.SignalEvent(sig)); 1 },
          "b" -> { () => Step.await[String]("b1", Awaitable.SignalEvent(sig)); 2 }
        )
      }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    rt.cancel(id)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended,
      "uncancellable suppresses the race's post-join cancellation delivery; the workflow suspends instead")
  }

  test("a race whose branches suspend after observing the cancellation propagates it") {
    val rt = newRuntime
    val innerStarted = new java.util.concurrent.CountDownLatch(1)
    var innerRaceCancelled = false
    val wf = Workflow[String, String](id = "lc-suspend") { _ =>
      val result = Step.firstCompletedWithoutSuspension[Int]("outer")(
        "winner" -> { () =>
          innerStarted.await(10, TimeUnit.SECONDS)
          Step.atLeastOnce[Int]("outer-winner") { 1 }
        },
        "loser" -> { () =>
          try {
            Step.firstCompletedWithoutSuspension[Int]("inner")(
              "i1" -> { () =>
                innerStarted.countDown()
                awaitCancellation(summon[WorkflowContext])
                TestControlFlow.suspend()
              },
              "i2" -> { () => TestControlFlow.suspend() }
            )
            2
          } catch {
            case e: WorkflowCancelledException =>
              innerRaceCancelled = true
              throw e
          }
        }
      )
      result.toString
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1"))
    assert(innerRaceCancelled, "the inner race must propagate the cancellation after its branches suspended")
    assert(!storedStepIds(wf.id, "k").contains("inner"))
  }
}
