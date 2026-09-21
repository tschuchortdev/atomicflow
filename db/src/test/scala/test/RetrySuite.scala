package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

class RetrySuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def stepRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String,
      stepVersion: Long
  ): Option[(String, String, Option[Instant])] =
    run(
      sql"""SELECT state_kind, state_payload, expires_at FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND step_id = $stepId AND step_version = $stepVersion""".query[
          (String, String, Option[Instant])
        ].option
    )

  private def timerSubscriptions(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Vector[(java.util.UUID, Instant)] =
    run(
      sql"""SELECT timer_id, deadline FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''
              AND step_id = 'step' AND step_version = 1""".query[
          (java.util.UUID, Instant)
        ].to[Vector]
    )

  test("inline retry: short delays sleep in-process and the step completes in one run") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 1.hour)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "inline") { in =>
      Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(3, 20.millis)) {
        val n = counter.incrementAndGet()
        if (n < 3) throw new RuntimeException("transient")
        "ok"
      }
    }

    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("ok"))
    assertEquals(counter.get(), 3)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("succeeded"))
  }

  test("durable retry: a long delay suspends with retry bookkeeping and a timer subscription; re-run after the deadline re-executes and completes") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "durable") { in =>
      Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(2, 1.hour)) {
        val n = counter.incrementAndGet()
        if (n == 1) throw new RuntimeException("boom")
        "recovered"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("started"))
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    assertEquals(subs.head._2, Instant.parse("2026-01-02T01:00:00Z"))

    clock.advanceBy(2.hours)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("recovered"))
    assertEquals(counter.get(), 2)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("succeeded"))
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the retry subscription is retired on success")
  }

  test("a retry not yet due suspends again without re-executing the body") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "notdue") { in =>
      Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(2, 1.hour)) {
        val n = counter.incrementAndGet()
        if (n < 3) throw new RuntimeException("boom")
        "ok"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)

    clock.advanceBy(30.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1, "the body must not re-execute before the retry deadline")
  }

  test("two consecutive durable retries each mint a fresh subscription; a re-run before the second delay stays suspended") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "two-retries") { in =>
      Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(3, 1.hour)) {
        val n = counter.incrementAndGet()
        if (n < 3) throw new RuntimeException("boom")
        "recovered"
      }
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    val sub1 = timerSubscriptions(wf.id, "k").head._1

    clock.advanceBy(2.hours)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "the second attempt executes after the first deadline")
    val subs2 = timerSubscriptions(wf.id, "k")
    assertEquals(subs2.length, 1, "exactly one retry subscription")
    assertNotEquals(subs2.head._1, sub1, "the second durable retry mints a fresh subscription id")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "before the second deadline the body must not re-execute")

    clock.advanceBy(2.hours)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("recovered"))
    assertEquals(counter.get(), 3)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the retry subscription is retired on success")
  }

  test("durable retry: after maxRetries are exhausted the failure is persisted and replayed without re-executing") {    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    var caught: Option[String] = None
    val wf = Workflow[String, String](id = "exhaust") { in =>
      try {
        Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(1, 1.hour)) {
          counter.incrementAndGet()
          throw new RuntimeException(s"boom-$in")
        }
      } catch {
        case e: Throwable => caught = Some(e.getMessage)
      } finally {
        TestControlFlow.suspend()
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    assertEquals(timerSubscriptions(wf.id, "k").length, 1)

    clock.advanceBy(2.hours)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "the retry re-executes once before exhaustion")
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("failed"))
    assert(caught.get.contains("boom-a"))

    rt.runWorkflowInstance(wf, id)
    assertEquals(counter.get(), 2, "replay of the exhausted failure must not re-execute")
    assert(caught.get.contains("boom-a"))
  }

  test("a non-retriable failure persists immediately with no retry subscription") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "nonretriable") { in =>
      try {
        Step.atLeastOnce[String](
          "step",
          retry = Step.RetryPolicy.fixedDelay(5, 1.hour, isRetriable = t => !t.getMessage.contains("fatal"))
        ) {
          counter.incrementAndGet()
          throw new RuntimeException("fatal-error")
        }
      } catch {
        case _: Throwable => ()
      } finally {
        TestControlFlow.suspend()
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("failed"))
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "no retry subscription for a non-retriable failure")
  }

  test("RetryPolicy.nextDelay: exponential backoff delays grow and cap at maxCumulativeDelay / maxRetries") {
    val p = Step.RetryPolicy.exponentialBackoff(initialDelay = 1.second, maxCumulativeDelay = 100.seconds, multiplier = 2)
    assertEquals(p.nextDelay(new RuntimeException, 0, 0.seconds, None), Some(1.second))
    assertEquals(p.nextDelay(new RuntimeException, 1, 1.second, Some(1.second)), Some(2.seconds))
    assertEquals(p.nextDelay(new RuntimeException, 2, 3.seconds, Some(2.seconds)), Some(4.seconds))

    val capped = Step.RetryPolicy.exponentialBackoff(initialDelay = 1.second, maxCumulativeDelay = 7.seconds)
    assertEquals(capped.nextDelay(new RuntimeException, 0, 0.seconds, None), Some(1.second))
    assertEquals(capped.nextDelay(new RuntimeException, 1, 1.second, Some(1.second)), Some(2.seconds))
    assertEquals(capped.nextDelay(new RuntimeException, 2, 3.seconds, Some(2.seconds)), Some(4.seconds))
    assertEquals(capped.nextDelay(new RuntimeException, 3, 7.seconds, Some(4.seconds)), None)

    val bounded = Step.RetryPolicy.exponentialBackoff(maxRetries = 2, initialDelay = 1.second)
    assertEquals(bounded.nextDelay(new RuntimeException, 0, 0.seconds, None), Some(1.second))
    assertEquals(bounded.nextDelay(new RuntimeException, 1, 1.second, Some(1.second)), Some(2.seconds))
    assertEquals(bounded.nextDelay(new RuntimeException, 2, 3.seconds, Some(2.seconds)), None)

    assertEquals(Step.RetryPolicy.never.nextDelay(new RuntimeException, 0, 0.seconds, None), None)

    val filtered = Step.RetryPolicy.fixedDelay(5, 1.second, isRetriable = _ => false)
    assertEquals(filtered.nextDelay(new RuntimeException, 0, 0.seconds, None), None)
  }

  test("crash during a retry body leaves the scheduled subscription untouched; re-running re-enters after the deadline with exactly one subscription") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "crashretry") { in =>
      Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(5, 1.hour)) {
        val n = counter.incrementAndGet()
        if (n == 1) throw new RuntimeException("first-fail")
        else TestControlFlow.suspend()
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val sub1 = timerSubscriptions(wf.id, "k")
    assertEquals(sub1.length, 1, "the durable retry schedules exactly one subscription")
    assertEquals(counter.get(), 1)

    clock.advanceBy(2.hours)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "the body re-runs after the deadline")
    assertEquals(timerSubscriptions(wf.id, "k"), sub1, "the same subscription survives; it is not double-scheduled")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 3, "re-running again re-enters the same scheduled retry")
    assertEquals(timerSubscriptions(wf.id, "k"), sub1)
  }

  test("invalidateAfter expires an ongoing durable retry; re-run executes fresh as if never begun") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ttlretry") { in =>
      Step.atLeastOnce[String]("step", invalidateAfter = 30.seconds, retry = Step.RetryPolicy.fixedDelay(2, 1.hour)) {
        counter.incrementAndGet()
        throw new RuntimeException("still-boom")
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    val sub1 = timerSubscriptions(wf.id, "k")
    assertEquals(sub1.length, 1)

    clock.advanceBy(1.minute)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "after the TTL the body executes fresh (as if never begun)")
    val sub2 = timerSubscriptions(wf.id, "k")
    assertEquals(sub2.length, 1, "a fresh retry subscription is scheduled")
    assertNotEquals(sub2.head._1, sub1.head._1, "the fresh retry mints a new subscription id")
  }

  test("getExecutionState reports Started while a durable retry is suspended") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    var observed: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "retrystate") { in =>
      observed = Step.getExecutionState[String]("step", stepVersion = 1)
      Step.atLeastOnce[String]("step", retry = Step.RetryPolicy.fixedDelay(2, 1.hour)) {
        throw new RuntimeException("boom")
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(observed, StepExecutionState.NeverStarted)

    clock.advanceBy(30.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(observed, StepExecutionState.Started, "during the durable retry suspension the step is Started")
  }
}
