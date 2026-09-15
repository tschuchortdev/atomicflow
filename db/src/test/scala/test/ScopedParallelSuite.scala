package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.atomic.AtomicInteger

class ScopedParallelSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def stepIds(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[String] =
    run(
      sql"""SELECT DISTINCT step_id FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key
            ORDER BY step_id""".query[String].to[Vector]
    )

  test("scoped runs the same step definition once per element with distinct rows") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "scoped-per-element") { in =>
      val results = List(1, 2).map(i =>
        Workflow.scoped(i) {
          Step.atLeastOnce[Int]("process") {
            counter.incrementAndGet()
            i * 10
          }
        }
      )
      results.mkString(",")
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("10,20"))
    assertEquals(counter.get(), 2, "each element's body executes exactly once")
    assertEquals(stepIds(wf.id, "k").size, 2, "one row per distinct scope")
  }

  test("scoped nesting joins the scope path with /") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "scoped-nest") { in =>
      Workflow.scoped("outer") {
        Workflow.scoped("inner") {
          Step.atLeastOnce[Int]("step") { 1 }
        }
      }
      "done"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("done"))
    assertEquals(stepIds(wf.id, "k"), Vector("outer/inner/step"))
  }

  test("Fingerprintable overload: equal elements share the derived scope (cached); distinct elements do not") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "fp-scope") { in =>
      Workflow.scoped(5) { Step.atLeastOnce[Int]("s") { counter.incrementAndGet(); 1 } }
      Workflow.scoped(5) { Step.atLeastOnce[Int]("s") { counter.incrementAndGet(); 1 } }
      Workflow.scoped(6) { Step.atLeastOnce[Int]("s") { counter.incrementAndGet(); 2 } }
      "done"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("done"))
    assertEquals(counter.get(), 2, "the second equal element reuses the cached step; the distinct element executes")
  }

  test("runToSuspension catches a suspension as Left and returns Right for completion") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "rts") { in =>
      val suspending: Either[WorkflowSuspendedException, String] = Workflow.runToSuspension {
        Step.await[String]("wait", Awaitable.SignalEvent(signal))
      }
      val completing: Either[WorkflowSuspendedException, Int] = Workflow.runToSuspension {
        Step.atLeastOnce[Int]("done") { 42 }
      }
      s"${suspending.isLeft}|${completing.toOption.get}"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("true|42"))
  }

  test("parallel runs all branches concurrently and returns results in order") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "par-happy") { in =>
      val results = Workflow.parallel(
        () => Step.atLeastOnce[Int]("a") { 1 },
        () => Step.atLeastOnce[Int]("b") { 2 },
        () => Step.atLeastOnce[Int]("c") { 3 }
      )
      results.mkString(",")
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("1,2,3"))
  }

  test("parallel collects completed results and a combined suspension; resume returns all results without re-running the completed branch") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "par-suspend") { in =>
      val results = Workflow.parallel(
        () => {
          Step.atLeastOnce[Int]("fast") { counter.incrementAndGet(); 1 }
        },
        () => {
          Step.await[String]("wait", Awaitable.SignalEvent(signal))
          "done"
        }
      )
      s"${results(0)}|${results(1)}"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1, "the fast branch completed once")
    signal.send(id, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1|done"))
    assertEquals(counter.get(), 1, "the fast branch's step is cached on resume")
  }

  test("parallel all-suspend produces a combined exception with one cause per branch") {
    val rt = newRuntime
    val s1 = Signal[String]("s1")
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "par-all-suspend") { in =>
      val caught = Workflow.runToSuspension {
        Workflow.parallel(
          () => Step.await[String]("a", Awaitable.SignalEvent(s1)),
          () => Step.await[String]("b", Awaitable.SignalEvent(s2))
        )
      }
      caught.left.toOption.map(_.causes.size).getOrElse(-1).toString
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("2"))
  }

  test("parallel branches with different scoped keys produce distinct step rows without cross-contamination") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "par-threadsafe") { in =>
      val results = Workflow.parallel(
        () => Workflow.scoped("A") { Step.atLeastOnce[String]("step") { "a" } },
        () => Workflow.scoped("B") { Step.atLeastOnce[String]("step") { "b" } },
        () => Workflow.scoped("C") { Step.atLeastOnce[String]("step") { "c" } }
      )
      results.mkString(",")
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("a,b,c"))
    assertEquals(stepIds(wf.id, "k"), Vector("A/step", "B/step", "C/step"))
  }

  test("plain sequential loops with scoped: foreach, map, and fold") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "loops") { in =>
      for i <- 1 to 3 do
        Workflow.scoped("f-" + i) { Step.atLeastOnce[Int]("step") { i } }
      val mapped = (1 to 3).map(i => Workflow.scoped("m-" + i) { Step.atLeastOnce[Int]("step") { i * 10 } })
      val folded = (1 to 3).foldLeft(0) { (acc, i) =>
        acc + Workflow.scoped("d-" + i) { Step.atLeastOnce[Int]("step") { i } }
      }
      s"${mapped.mkString(",")}|$folded"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("10,20,30|6"))
  }
}
