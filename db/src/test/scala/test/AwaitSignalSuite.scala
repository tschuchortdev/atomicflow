package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class AwaitSignalSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def cursor(workflowId: WorkflowId, key: WorkflowInstanceKey, signalKey: SignalKey): Option[Long] =
    run(
      sql"""SELECT sequence_id FROM signal_cursor
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND signal_key = $signalKey""".query[Long].option
    )

  private def subscriptions(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[(String, Long, String, String)] =
    run(
      sql"""SELECT step_id, step_version, subscriber_key, signal_key FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[(String, Long, String, String)].to[Vector]
    )

  private def stepRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String
  ): Option[(String, String, String)] =
    run(
      sql"""SELECT step_kind, state_kind, state_payload FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND step_id = $stepId AND step_version = 0""".query[
          (String, String, String)
        ].option
    )

  test("await suspends when no event is available, leaving a pending subscription") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "await-suspend") { in =>
      Step.await[String]("wait-greet", Awaitable.SignalEvent(signal))
      "done"
    }
    rt.createWorkflowInstance(wf, "k", "in")

    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.WorkflowSuspended)

    val subs = subscriptions(wf.id, "k")
    assertEquals(subs, Vector(("wait-greet", 0L, "", "greet")))
    assertEquals(cursor(wf.id, "k", "greet"), None)
  }

  test("send then re-run resolves the await and the workflow completes with the decoded value") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "await-resolve") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(signal.send(id, "hello")(using rt), SignalSendResult.Success)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("got:hello"))
    assertEquals(subscriptions(wf.id, "k"), Vector.empty)
    val (kind, state, payload) = stepRow(wf.id, "k", "wait").get
    assertEquals(kind, "Await")
    assertEquals(state, "Succeeded")
    assertEquals(payload, "hello")
  }

  test("steps before the await are not re-executed on resume; the await result is cached; terminal re-run does not suspend") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "await-cache") { in =>
      Step.atLeastOnce[String]("before") {
        counter.incrementAndGet()
        "pre"
      }
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "final"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)

    assertEquals(signal.send(id, "hello")(using rt), SignalSendResult.Success)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("final"))
    assertEquals(counter.get(), 1, "the step before the await must be cached on resume")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("final"))
    assertEquals(stepRow(wf.id, "k", "wait").get._2, "Succeeded")
  }

  test("a filter-rejected event is skipped permanently: the cursor jumps to the accepted event") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "await-skip") { in =>
      val v1 = Step.await[String]("a1", Awaitable.SignalEvent(signal, filter = _ == "accept"))
      val v2 = Step.await[String]("a2", Awaitable.SignalEvent(signal))
      s"$v1|$v2"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "reject1")(using rt)
    signal.send(id, "accept")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val c = cursor(wf.id, "k", "s").get
    val acceptSeq = run(
      sql"""SELECT sequence_id FROM workflow_events WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''
            AND event_kind = 'Signal' AND event_key = 's' AND payload = 'accept'""".query[Long].unique
    )
    assertEquals(c, acceptSeq)

    signal.send(id, "next")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("accept|next"))
  }

  test("when all events are rejected the cursor stays unchanged and a later matching event can satisfy the await") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "await-reject") { in =>
      Step.await[String]("a", Awaitable.SignalEvent(signal, filter = _ == "good"))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "bad1")(using rt)
    signal.send(id, "bad2")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cursor(wf.id, "k", "s"), None)

    signal.send(id, "good")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("lookBack ignores events older than the window using the event's original created_at") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "await-lookback") { in =>
      Step.await[String]("a", Awaitable.SignalEvent(signal, lookBack = 5.seconds))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "old")(using rt)
    clock.advanceBy(10.seconds)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    signal.send(id, "fresh")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    val (kind, state, payload) = stepRow(wf.id, "k", "a").get
    assertEquals((kind, state, payload), ("Await", "Succeeded", "fresh"))
  }

  test("peekSignal returns visible events after the cursor without advancing it") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "await-peek") { in =>
      val peeked = Step.peekSignal(signal)
      val awaited = Step.await[String]("a", Awaitable.SignalEvent(signal))
      s"${peeked.mkString(",")}|$awaited"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "x1")(using rt)
    signal.send(id, "x2")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("x1,x2|x1"))
  }

  test("invalidateOn discards the cached await result and re-evaluates when the dependency changes") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    var ctx = "A"
    var last = ""
    val wf = Workflow[String, String](id = "await-invalidate") { in =>
      last = Step.await[String]("a", Awaitable.SignalEvent(signal), invalidateOn = Seq("ctx" -> ctx))
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "e1")(using rt)
    rt.runWorkflowInstance(wf, id)
    assertEquals(last, "e1")
    rt.runWorkflowInstance(wf, id)
    assertEquals(last, "e1", "the cached result is reused while the dependency is unchanged")

    ctx = "B"
    signal.send(id, "e2")(using rt)
    rt.runWorkflowInstance(wf, id)
    assertEquals(last, "e2")
  }

  test("ensureUnchanged conflict on an await raises StepInputConflictException") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    var ctx = "A"
    var conflict: Option[String] = None
    val wf = Workflow[String, String](id = "await-conflict") { in =>
      try {
        Step.await[String]("a", Awaitable.SignalEvent(signal), ensureUnchanged = Seq("ctx" -> ctx))
      } catch {
        case e: StepInputConflictException => conflict = Some(e.getMessage)
      }
      TestControlFlow.suspend()
      "x"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "e1")(using rt)
    rt.runWorkflowInstance(wf, id)
    assertEquals(conflict, None)

    ctx = "B"
    rt.runWorkflowInstance(wf, id)
    assert(conflict.isDefined)
  }

  test("invalidateAfter expires the cached await result and re-evaluates after the TTL") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val signal = Signal[String]("s")
    var last = ""
    val wf = Workflow[String, String](id = "await-ttl") { in =>
      last = Step.await[String]("a", Awaitable.SignalEvent(signal), invalidateAfter = 10.seconds)
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "e1")(using rt)
    rt.runWorkflowInstance(wf, id)
    assertEquals(last, "e1")
    rt.runWorkflowInstance(wf, id)
    assertEquals(last, "e1", "the result is cached before the TTL expires")

    clock.advanceBy(11.seconds)
    signal.send(id, "e2")(using rt)
    rt.runWorkflowInstance(wf, id)
    assertEquals(last, "e2")
  }

  test("an event sent from a step body during the run is resolved by the same run's await") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "await-during") { in =>
      val ctx = summon[WorkflowContext]
      Step.atLeastOnce[Unit]("emit") {
        signal.send(ctx.instanceId, "mid")(using ctx.runtime)
        ()
      }
      val v = Step.await[String]("a", Awaitable.SignalEvent(signal))
      s"emit:$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("emit:mid"))
  }

  test("two sequential awaits of the same key consume successive events") {
    val rt = newRuntime
    val signal = Signal[String]("s")
    val wf = Workflow[String, String](id = "await-sequential") { in =>
      val a = Step.await[String]("a1", Awaitable.SignalEvent(signal))
      val b = Step.await[String]("a2", Awaitable.SignalEvent(signal))
      s"$a|$b"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signal.send(id, "x1")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    signal.send(id, "x2")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("x1|x2"))
  }
}
