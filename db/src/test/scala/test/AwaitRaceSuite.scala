package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Clock
import java.time.Instant
import scala.concurrent.duration.*

sealed trait RaceOutcome
case object RaceOutcomeTimeout extends RaceOutcome
final case class RaceOutcomeGot(v: String) extends RaceOutcome

object RaceOutcome {
  given Cacheable[RaceOutcome] = new Cacheable[RaceOutcome] {
    override def stableSerializedTypeId: String = "race-outcome"
    override def write(value: RaceOutcome): String = value match {
      case RaceOutcomeTimeout => "timeout"
      case RaceOutcomeGot(x)  => s"got:$x"
    }
    override def read(serialized: String): RaceOutcome =
      if (serialized == "timeout") RaceOutcomeTimeout
      else RaceOutcomeGot(serialized.stripPrefix("got:"))
  }
}

class AwaitRaceSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def signalSubscriptions(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[(String, Long, String, String)] =
    run(
      sql"""SELECT step_id, step_version, subscriber_key, signal_key FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''
            ORDER BY step_id, subscriber_key""".query[(String, Long, String, String)].to[Vector]
    )

  private def timerSubscriptions(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[(String, Long, String, java.util.UUID, Instant)] =
    run(
      sql"""SELECT step_id, step_version, subscriber_key, timer_id, deadline FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''
            ORDER BY step_id, subscriber_key""".query[(String, Long, String, java.util.UUID, Instant)].to[Vector]
    )

  private def completionSubscriptions(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Vector[(String, Long, String, String, String, String)] =
    run(
      sql"""SELECT step_id, step_version, subscriber_key, completed_workflow_id, completed_workflow_instance_key, completed_scope
            FROM workflow_completion_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''
            ORDER BY step_id, subscriber_key""".query[(String, Long, String, String, String, String)].to[Vector]
    )

  private def cursor(workflowId: WorkflowId, key: WorkflowInstanceKey, signalKey: SignalKey): Option[Long] =
    run(
      sql"""SELECT sequence_id FROM signal_cursor
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND signal_key = $signalKey""".query[Long].option
    )

  private def seqOf(workflowId: WorkflowId, key: WorkflowInstanceKey, signalKey: SignalKey, payload: String): Long =
    run(
      sql"""SELECT sequence_id FROM workflow_events
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''
              AND event_kind = 'Signal' AND event_key = $signalKey AND payload = $payload""".query[Long].unique
    )

  private def timerFiredOrder(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[String] =
    run(
      sql"""SELECT event_key FROM workflow_events
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''
              AND event_kind = 'TimerFired' ORDER BY sequence_id""".query[String].to[Vector]
    )

  private def wakeupExists(workflowId: WorkflowId, key: WorkflowInstanceKey): Boolean =
    run(
      sql"""SELECT EXISTS(SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '')""".query[Boolean].unique
    )

  private def stepRow(workflowId: WorkflowId, key: WorkflowInstanceKey, stepId: String): Option[(String, String, String)] =
    run(
      sql"""SELECT step_kind, state_kind, state_payload FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND step_id = $stepId AND step_version = 0""".query[
          (String, String, String)
        ].option
    )

  test("all-unsatisfiable race suspends with every leaf's subscription registered and no cursor movement") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("s")
    val wf = Workflow[String, String](id = "race-suspend") { in =>
      Step.awaitRace[String]("race")("signal" -> Awaitable.SignalEvent(sig), "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout"))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(signalSubscriptions(wf.id, "k"), Vector(("race", 0L, "signal", "s")))
    assertEquals(timerSubscriptions(wf.id, "k").map { case (s, v, l, _, _) => (s, v, l) }, Vector(("race", 0L, "timeout")))
    assertEquals(completionSubscriptions(wf.id, "k"), Vector.empty)
    assertEquals(cursor(wf.id, "k", "s"), None)
    assertEquals(timerFiredOrder(wf.id, "k"), Vector.empty)
  }

  test("signal sent first wins the race over an also-due timer; only the signal cursor advances") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("s")
    var last = ""
    val wf = Workflow[String, String](id = "race-signal-wins") { in =>
      last = Step.awaitRace[String]("race")("signal" -> Awaitable.SignalEvent(sig), "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout"))
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    sig.send(id, "hello")(using rt)
    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    assertEquals(last, "hello")
    assertEquals(cursor(wf.id, "k", "s").get, seqOf(wf.id, "k", "s", "hello"))
    assertEquals(signalSubscriptions(wf.id, "k"), Vector.empty, "both leaves' subscriptions are deleted on resolution")
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty)
    assertEquals(timerFiredOrder(wf.id, "k").length, 1, "the due timer leaf was materialized but lost the race")
  }

  test("a timer that resolves before a later signal keeps the cached timer outcome") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("s")
    var last = ""
    val wf = Workflow[String, String](id = "race-timer-wins") { in =>
      last = Step.awaitRace[String]("race")("signal" -> Awaitable.SignalEvent(sig), "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout"))
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "timeout")
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty)

    sig.send(id, "hello")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "timeout", "the cached race result wins even though the signal arrived later")
  }

  test("due timers of a race are materialized in deadline order; the earliest due timer wins") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    var last = ""
    val wf = Workflow[String, String](id = "race-timers") { in =>
      last = Step.awaitRace[String]("race")(
        "t5" -> Awaitable.Timer(5.minutes).map(_ => "t5"),
        "t1" -> Awaitable.Timer(1.minute).map(_ => "t1")
      )
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val (_, _, _, subT1, _) = timerSubscriptions(wf.id, "k")(0)
    val (_, _, _, subT5, _) = timerSubscriptions(wf.id, "k")(1)

    clock.advanceBy(6.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    assertEquals(last, "t1", "the earliest deadline's fired event has the lowest sequenceId and wins")
    assertEquals(timerFiredOrder(wf.id, "k"), Vector(subT1.toString, subT5.toString))
  }

  test("only the winning signal key's cursor advances; losing key's events survive for later awaits") {
    val rt = newRuntime
    val sigA = Signal[String]("a")
    val sigB = Signal[String]("b")
    val wf = Workflow[String, String](id = "race-two-sig") { in =>
      val r = Step.awaitRace[String]("race")("a" -> Awaitable.SignalEvent(sigA), "b" -> Awaitable.SignalEvent(sigB))
      val b = Step.await[String]("after", Awaitable.SignalEvent(sigB))
      s"$r|$b"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(signalSubscriptions(wf.id, "k"), Vector(("race", 0L, "a", "a"), ("race", 0L, "b", "b")))

    sigA.send(id, "xa")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(cursor(wf.id, "k", "a").get, seqOf(wf.id, "k", "a", "xa"))
    assertEquals(cursor(wf.id, "k", "b"), None, "the losing signal key's cursor must not advance")
    assertEquals(signalSubscriptions(wf.id, "k"), Vector(("after", 0L, "", "b")), "the race's own leaves are retired")

    sigB.send(id, "yb")(using rt)
    val ybSeq = seqOf(wf.id, "k", "b", "yb")
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("xa|yb"))
    assertEquals(cursor(wf.id, "k", "b").get, ybSeq)
  }

  test("a resolved race is cached: re-runs do not re-evaluate or re-register subscriptions") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("s")
    var last = ""
    val wf = Workflow[String, String](id = "race-cached") { in =>
      last = Step.awaitRace[String]("race")("signal" -> Awaitable.SignalEvent(sig), "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout"))
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    sig.send(id, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "x")
    assertEquals(stepRow(wf.id, "k", "race").map(_._2), Some("succeeded"))

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "x", "the cached result is reused rather than re-evaluating to the due timer")
    assertEquals(timerFiredOrder(wf.id, "k"), Vector.empty, "no timer leaf is fired for a cached race")
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty)
  }

  test("invalidateOn change discards the cached race and re-evaluates from scratch, re-registering subscriptions") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("s")
    var ctx = "A"
    var last = ""
    val wf = Workflow[String, String](id = "race-invalidate") { in =>
      last = Step.awaitRace[String]("race", invalidateOn = Seq("ctx" -> ctx))(
        "signal" -> Awaitable.SignalEvent(sig),
        "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout")
      )
      TestControlFlow.suspend()
      last
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    sig.send(id, "e1")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "e1")
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "e1", "the cached result is reused while the dependency is unchanged")

    ctx = "B"
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(
      signalSubscriptions(wf.id, "k").map { case (s, v, l, k) => (s, v, l, k) },
      Vector(("race", 0L, "signal", "s")),
      "the invalidated race re-registers its subscriptions"
    )
    assertEquals(
      timerSubscriptions(wf.id, "k").map(_._3),
      Vector("timeout"),
      "the invalidated race re-registers its timer member"
    )

    sig.send(id, "e2")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "e2")
  }

  test("heterogeneous leaves unify onto one result type via map") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("s")
    val wf = Workflow[String, String](id = "race-het") { in =>
      val v = Step.awaitRace[RaceOutcome]("race")(
        "timeout" -> Awaitable.Timer(1.minute).map(_ => RaceOutcomeTimeout),
        "signal" -> Awaitable.SignalEvent(sig).map(v => RaceOutcomeGot(v))
      )
      v match {
        case RaceOutcomeTimeout    => "timeout"
        case RaceOutcomeGot(x)     => s"got:$x"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    sig.send(id, "hello")(using rt)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("got:hello"))
    assertEquals(stepRow(wf.id, "k", "race").map(_._2), Some("succeeded"))
  }

  test("completion await: A suspends on B's completion; B completing wakes A; re-run resolves Completed(value)") {
    val rt = newRuntime
    val bWf = Workflow[String, String](id = "B") { in => in.toUpperCase }
    val bId = rt.createWorkflowInstance(bWf, "b", "hello").id
    val bHandle = rt.getWorkflowInstance(bWf, bId)

    val aWf = Workflow[String, String](id = "A") { in =>
      val comp = Step.await[WorkflowCompletionResult[String]]("wait-b", bHandle.completion)
      comp match {
        case WorkflowCompletionResult.Completed(v) => s"got:$v"
        case _                                     => "?"
      }
    }
    val aId = rt.createWorkflowInstance(aWf, "a", "in").id

    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(completionSubscriptions(aWf.id, "a"), Vector(("wait-b", 0L, "", "B", "b", "")))

    assertEquals(rt.runWorkflowInstance(bWf, bId), WorkflowRunResult.Result("HELLO"))
    assert(wakeupExists(aWf.id, "a"), "A's wakeup is upserted when B's terminal transition commits")

    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.Result("got:HELLO"))
    assertEquals(completionSubscriptions(aWf.id, "a"), Vector.empty)
  }

  test("completion await resolves Failed(decoded throwable) when the awaited instance fails") {
    val rt = newRuntime
    val bWf = Workflow[String, String](id = "Bfail") { in => throw new RuntimeException("boom") }
    val bId = rt.createWorkflowInstance(bWf, "bf", "in").id
    val bHandle = rt.getWorkflowInstance(bWf, bId)

    val aWf = Workflow[String, String](id = "A2") { in =>
      val comp = Step.await[WorkflowCompletionResult[String]]("wait-b", bHandle.completion)
      comp match {
        case WorkflowCompletionResult.Failed(t) => s"failed:${t.getMessage}"
        case _                                  => "?"
      }
    }
    val aId = rt.createWorkflowInstance(aWf, "a2", "in").id

    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    intercept[RuntimeException] { rt.runWorkflowInstance(bWf, bId) }
    val failed = rt.runWorkflowInstance(aWf, aId)
    assert(failed.toString.contains("failed:") && failed.toString.contains("boom"), s"decoded failure message: $failed")
  }

  test("a mapped completion leaf raced against a timer resolves with the completion when it completes first") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val bWf = Workflow[String, String](id = "Bmap") { in => in.toUpperCase }
    val bId = rt.createWorkflowInstance(bWf, "bm", "hello").id
    val bHandle = rt.getWorkflowInstance(bWf, bId)

    var last = ""
    val aWf = Workflow[String, String](id = "Amap") { in =>
      last = Step.awaitRace[String]("race")(
        "completion" -> bHandle.completion.map {
          case WorkflowCompletionResult.Completed(v) => v
          case _                                     => "?"
        },
        "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout")
      )
      TestControlFlow.suspend()
      last
    }
    val aId = rt.createWorkflowInstance(aWf, "a", "in").id

    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(bWf, bId), WorkflowRunResult.Result("HELLO"))
    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "HELLO", "the mapped completion result wins when it completes first")
    assertEquals(stepRow(aWf.id, "a", "race").map(_._2), Some("succeeded"))

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "HELLO", "the resolved mapped completion is cached and replayed, not re-raced")
  }

  test("a mapped completion leaf raced against a timer resolves with the timer when it fires first") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val bWf = Workflow[String, String](id = "Bmap2") { in => in.toUpperCase }
    val bId = rt.createWorkflowInstance(bWf, "bm2", "hello").id
    val bHandle = rt.getWorkflowInstance(bWf, bId)

    var last = ""
    val aWf = Workflow[String, String](id = "Amap2") { in =>
      last = Step.awaitRace[String]("race")(
        "completion" -> bHandle.completion.map {
          case WorkflowCompletionResult.Completed(v) => v
          case _                                     => "?"
        },
        "timeout" -> Awaitable.Timer(1.minute).map(_ => "timeout")
      )
      TestControlFlow.suspend()
      last
    }
    val aId = rt.createWorkflowInstance(aWf, "a", "in").id

    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, "timeout", "the timer wins when it fires before the completion")
  }

  test("an unmapped completion leaf raced against a signal resolves with the earliest global event") {
    val rt = newRuntime
    val sig = Signal[String]("s")
    val bWf = Workflow[String, String](id = "Bmix") { in => in.toUpperCase }
    val bId = rt.createWorkflowInstance(bWf, "bmix", "hi").id
    val bHandle = rt.getWorkflowInstance(bWf, bId)

    var last: WorkflowCompletionResult[String] = WorkflowCompletionResult.Cancelled
    val aWf = Workflow[String, String](id = "Amix") { in =>
      last = Step.awaitRace[WorkflowCompletionResult[String]]("race")(
        "completion" -> bHandle.completion,
        "signal" -> Awaitable.SignalEvent(sig).map(v => WorkflowCompletionResult.Completed(v))
      )
      TestControlFlow.suspend()
      "done"
    }
    val aId = rt.createWorkflowInstance(aWf, "a", "in").id

    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(bWf, bId), WorkflowRunResult.Result("HI"))

    sig.send(aId, "sx")(using rt)
    assertEquals(rt.runWorkflowInstance(aWf, aId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(last, WorkflowCompletionResult.Completed("HI"), "the earlier completion event beats the later signal")
  }

  test("awaitRace rejects empty, duplicate, and reserved-prefixed member keys") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    given Clock = clock
    val rt = newRuntime(clock)
    def runBody(id: String, members: Seq[(String, Awaitable[String])]): Unit = {
      val wf = Workflow[String, String](id = id) { in =>
        Step.awaitRace[String]("race")(members*)
        "done"
      }
      val instance = rt.createWorkflowInstance(wf, "k", "in").id
      rt.runWorkflowInstance(wf, instance)
    }
    val sig = Signal[String]("s")

    val empty = intercept[StepFailed] {
      runBody("race-invalid-empty", Seq("" -> Awaitable.SignalEvent(sig)))
    }
    assert(empty.getMessage.contains("non-empty"), empty.getMessage)

    val duplicate = intercept[StepFailed] {
      runBody("race-invalid-duplicate", Seq("dup" -> Awaitable.SignalEvent(sig), "dup" -> Awaitable.Timer(1.minute).map(_ => "timeout")))
    }
    assert(duplicate.getMessage.contains("unique"), duplicate.getMessage)

    val reserved = intercept[StepFailed] {
      runBody("race-invalid-reserved", Seq("__retry__" -> Awaitable.Timer(1.minute).map(_ => "timeout")))
    }
    assert(reserved.getMessage.contains("reserved"), reserved.getMessage)
  }
}
