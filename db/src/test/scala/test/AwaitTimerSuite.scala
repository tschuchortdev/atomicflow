package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Clock
import java.time.Instant
import scala.concurrent.duration.*

class AwaitTimerSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def timerSubscriptions(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Vector[(java.util.UUID, Instant)] =
    run(
      sql"""SELECT subscription_id, deadline FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[
          (java.util.UUID, Instant)
        ].to[Vector]
    )

  private def timerFiredEvents(workflowId: WorkflowId, key: WorkflowInstanceKey): Vector[String] =
    run(
      sql"""SELECT event_key FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''
              AND event_kind = 'TimerFired' ORDER BY sequence_id""".query[String].to[Vector]
    )

  private def timerFiredCount(subscriptionId: java.util.UUID): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_events
            WHERE event_kind = 'TimerFired' AND event_key = ${subscriptionId.toString}""".query[Int].unique
    )

  test("a timer not yet due suspends and records an absolute-deadline subscription with no event") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-notdue") { in =>
      Step.await[Unit]("t", Awaitable.Timer(5.minutes))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    assertEquals(subs.head._2, Instant.parse("2026-01-02T00:05:00Z"))
    assertEquals(timerFiredEvents(wf.id, "k"), Vector.empty)
  }

  test("advancing past the deadline then re-running fires the timer inline and completes") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-resolve") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subId = timerSubscriptions(wf.id, "k").head._1

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(timerFiredEvents(wf.id, "k"), Vector(subId.toString))
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the subscription is retired on resolution")
    assertEquals(timerFiredCount(subId), 1)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(timerFiredCount(subId), 1, "a terminal re-run must not fire again")
  }

  test("the deadline is stored once and never recomputed across re-runs") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-deadline") { in =>
      Step.await[Unit]("t", Awaitable.Timer(5.minutes))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val d1 = timerSubscriptions(wf.id, "k").head._2
    assertEquals(d1, Instant.parse("2026-01-02T00:05:00Z"))

    clock.advanceBy(1.minute)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val d2 = timerSubscriptions(wf.id, "k").head._2
    assertEquals(d2, d1, "the deadline must not be recomputed on replay")

    clock.advanceBy(6.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("exactly-once: firing never duplicates across repeated runs of a suspended flow") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-once") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subId = timerSubscriptions(wf.id, "k").head._1

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerFiredCount(subId), 1)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerFiredCount(subId), 1, "re-running a suspended-then-resolved flow must not fire again")
  }

  test("the subscription persists while suspended and is deleted only when the await resolves") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-lifecycle") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k").length, 1, "the subscription exists while suspended")

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the subscription is deleted when the await resolves")
    assertEquals(timerFiredEvents(wf.id, "k").length, 1, "the fired event survives the subscription")
  }

  test("manual mode resolves a timer purely via run evaluation, with no sweep or runner") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-manual") { in =>
      Step.await[Unit]("t", Awaitable.Timer(10.seconds))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    clock.advanceBy(20.seconds)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("invalidateOn re-registers a fresh subscription; the old TimerFired is inert") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    var ctx = "A"
    val wf = Workflow[String, String](id = "timer-invalidate") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute), invalidateOn = Seq("ctx" -> ctx))
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val (sub1, _) = timerSubscriptions(wf.id, "k").head

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerFiredCount(sub1), 1, "the old timer fired")
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the resolved await retired sub1")

    ctx = "B"
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    val (sub2, d2) = subs.head
    assertNotEquals(sub2, sub1, "a fresh subscription id is minted on invalidation")
    assertEquals(d2, Instant.parse("2026-01-02T00:03:00Z"), "the deadline recomputes to now + delay")
    assertEquals(timerFiredCount(sub2), 0, "the new subscription has no event of its own")
    assertEquals(timerFiredCount(sub1), 1, "the old event remains but is not reused")
  }

  test("invalidateAfter expiry re-registers a fresh subscription with a recomputed deadline; the old TimerFired is inert") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "timer-ttl") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute), invalidateAfter = 10.seconds)
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val (sub1, _) = timerSubscriptions(wf.id, "k").head

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerFiredCount(sub1), 1, "the old timer fired and resolved before the TTL expired")
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "resolution retired sub1")

    clock.advanceBy(20.seconds)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    val (sub2, d2) = subs.head
    assertNotEquals(sub2, sub1, "a fresh subscription is minted after the TTL expires")
    assertEquals(d2, Instant.parse("2026-01-02T00:03:20Z"), "the deadline recomputes to now + delay")
    assertEquals(timerFiredCount(sub2), 0, "the fresh subscription has no event of its own")
    assertEquals(timerFiredCount(sub1), 1, "the old event remains but is inert")
  }
}
