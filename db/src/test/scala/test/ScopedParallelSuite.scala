package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.util.concurrent.atomic.AtomicInteger

class ScopedParallelSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def stepScopePaths(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[String] =
    run(
      sql"""SELECT DISTINCT step_scope_path FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key
            ORDER BY step_scope_path""".query[String].to[Vector]
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
    assertEquals(stepScopePaths(wf.id, "k").size, 2, "one row per distinct scope")
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
    assertEquals(stepScopePaths(wf.id, "k"), Vector("outer/inner"))
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
        Step.atLeastOnce[Int]("a") { 1 },
        Step.atLeastOnce[Int]("b") { 2 },
        Step.atLeastOnce[Int]("c") { 3 }
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
        {
          Step.atLeastOnce[Int]("fast") { counter.incrementAndGet(); 1 }
        },
        {
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
          Step.await[String]("a", Awaitable.SignalEvent(s1)),
          Step.await[String]("b", Awaitable.SignalEvent(s2))
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
        Workflow.scoped("A") { Step.atLeastOnce[String]("step") { "a" } },
        Workflow.scoped("B") { Step.atLeastOnce[String]("step") { "b" } },
        Workflow.scoped("C") { Step.atLeastOnce[String]("step") { "c" } }
      )
      results.mkString(",")
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("a,b,c"))
    assertEquals(stepScopePaths(wf.id, "k"), Vector("A", "B", "C"))
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

  test("injectivity: StepId('a/b','') and StepId('b','a') are distinct rows; step_id is the bare key and the path lives in step_scope_path") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "scope-injective") { in =>
      Step.atLeastOnce[Int]("a/b") { counter.incrementAndGet(); 1 }
      Workflow.scoped("a") { Step.atLeastOnce[Int]("b") { counter.incrementAndGet(); 2 } }
      "done"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("done"))
    assertEquals(counter.get(), 2, "the two distinct step identities execute independently")
    val rows = run(
      sql"""SELECT step_id, step_scope_path FROM workflow_steps
            WHERE workflow_id = ${wf.id} AND key = 'k'
            ORDER BY step_id, step_scope_path""".query[(String, String)].to[Vector]
    )
    assertEquals(rows, Vector(("a/b", ""), ("b", "a")))
  }

  test("scoped requires a non-empty scope key") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "scoped-empty") { in =>
      try {
        Workflow.scoped("") { Step.atLeastOnce[Int]("s") { 1 } }
        "no-require"
      } catch {
        case _: IllegalArgumentException => "required"
      }
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("required"))
  }

  test("WorkflowNonFatal excludes the combined parallel suspension") {
    val rt = newRuntime
    val s1 = Signal[String]("s1")
    val s2 = Signal[String]("s2")
    val wf = Workflow[String, String](id = "par-nonfatal") { in =>
      Workflow.runToSuspension {
        Workflow.parallel(
          Step.await[String]("a", Awaitable.SignalEvent(s1)),
          Step.await[String]("b", Awaitable.SignalEvent(s2))
        )
      } match {
        case Left(combined) =>
          combined match {
            case WorkflowNonFatal(_) => "matched"
            case _                   => "not-matched"
          }
        case Right(_) => "completed"
      }
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("not-matched"))
  }

  test("parallel propagates a non-suspension failure") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "par-failure") { in =>
      try {
        Workflow.parallel(
          Step.atLeastOnce[Int]("ok") { 1 },
          Step.atLeastOnce[Int]("bad") { throw new RuntimeException("boom"); 2 }
        )
        "no-failure"
      } catch {
        case _: RuntimeException => "caught"
      }
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("caught"))
  }

  test("uncancellable in one parallel branch does not suppress cancellation in another branch") {
    val rt = newRuntime
    val shieldedCounter = new AtomicInteger(0)
    val exposedCounter = new AtomicInteger(0)
    val sig = Signal[String]("gate")
    val wf = Workflow[String, String](id = "par-uncancellable") { in =>
      Workflow.uncancellable {
        Step.await[String]("gate", Awaitable.SignalEvent(sig))
      }
      val results = Workflow.parallel(
        Workflow.uncancellable {
          Step.atLeastOnce[Int]("shielded") { shieldedCounter.incrementAndGet(); 1 }
        },
        {
          try {
            Step.atLeastOnce[Int]("exposed") { exposedCounter.incrementAndGet(); 2 }
            "exposed-completed"
          } catch {
            case _: WorkflowCancelledException => "exposed-cancelled"
          }
        }
      )
      s"${results(0)}|${results(1)}"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(shieldedCounter.get(), 0, "nothing runs before the gate opens")

    rt.cancel(id)
    sig.send(id, "go")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("1|exposed-cancelled"))
    assertEquals(shieldedCounter.get(), 1, "the shielded branch's body ran despite the pending cancel")
    assertEquals(exposedCounter.get(), 0, "the exposed branch's body never ran")
    val rows = run(
      sql"""SELECT step_id, step_scope_path, state_kind FROM workflow_steps
            WHERE workflow_id = ${wf.id} AND key = 'k'
            ORDER BY step_id, step_scope_path""".query[(String, String, String)].to[Vector]
    )
    assert(rows.exists { case (stepId, _, _) => stepId == "shielded" }, "the shielded step row exists (its checkpoint suppressed delivery)")
    assert(!rows.exists { case (stepId, _, _) => stepId == "exposed" }, "the exposed step row is absent (its body never ran)")
  }

  test("a step applied from a captured context on a foreign thread records the enclosing scope") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "foreign-thread-scope") { in =>
      Workflow.scoped("outer") {
        val body: WorkflowContext ?=> String =
          Step.atLeastOnce[String]("foreign") { counter.incrementAndGet(); "v" }
        val ctx = summon[WorkflowContext]
        val thread = new Thread(() => { body(using ctx); () })
        thread.start()
        thread.join()
      }
      "done"
    }
    rt.createWorkflowInstance(wf, "k", "in")
    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("done"))
    assertEquals(counter.get(), 1, "the step body executes exactly once")
    assertEquals(
      stepScopePaths(wf.id, "k"),
      Vector("outer"),
      "the step row lands under the enclosing scope carried by the context, not the foreign thread's empty scope"
    )
  }

  test("uncancellable suppression travels with the context onto a foreign thread") {
    val rt = newRuntime
    val sig = Signal[String]("gate")
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "foreign-thread-uncancellable") { in =>
      Workflow.uncancellable {
        Step.await[String]("gate", Awaitable.SignalEvent(sig))
        val body: WorkflowContext ?=> String =
          Step.atLeastOnce[String]("compensate") { counter.incrementAndGet(); "ok" }
        val ctx = summon[WorkflowContext]
        val thread = new Thread(() => try { body(using ctx); () } catch { case _: WorkflowCancelledException => () })
        thread.start()
        thread.join()
      }
      Step.atLeastOnce[Int]("after") { 2 }
      "finished"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    rt.cancel(id)
    sig.send(id, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowCancelled)
    assertEquals(
      counter.get(),
      1,
      "the compensation step executes despite the pending cancel: the uncancellable depth travels with the context"
    )
  }
}
