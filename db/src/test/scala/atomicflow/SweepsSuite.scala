package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.{Clock, Instant}
import scala.concurrent.duration.*

/** Background sweeps: timer firing (Path 1), cancellation escalation, and lease
  * recovery. Lives in package `atomicflow` so it can invoke the runner's
  * package-private sweep hooks (`runTimerSweep` etc.) and the loop-less
  * `startJobRunnerForTests` deterministically.
  */
class SweepsSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def waitUntil(cond: => Boolean, timeout: FiniteDuration = 10.seconds): Unit = {
    val deadline = System.nanoTime() + timeout.toNanos
    while (!cond && System.nanoTime() < deadline) Thread.sleep(10)
    assert(cond, "condition not met within timeout")
  }

  private def wakeupExists(workflowId: WorkflowId, key: WorkflowInstanceKey): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[Int].option
    ).isDefined

  private def terminalRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Option[String])] =
    run(
      sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[
          (Option[String], Option[String])
        ].option
    )

  private def leaseRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Option[Instant], Long)] =
    run(
      sql"""SELECT lease_owner, lease_expires_at, fencing_token FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[
          (Option[String], Option[Instant], Long)
        ].option
    )

  private def cancelRequestedAt(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""SELECT cancel_requested_at FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[Option[Instant]].unique
    )

  private def completedEvent(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[(String, String, String)] =
    run(
      sql"""SELECT event_kind, event_key, payload FROM workflow_events
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND event_kind = 'WorkflowCompleted'""".query[
          (String, String, String)
        ].option
    )

  private def timerFiredEvents(workflowId: WorkflowId): Vector[(String, Long)] =
    run(
      sql"""SELECT event_key, sequence_id FROM workflow_events
            WHERE workflow_id = $workflowId AND event_kind = 'TimerFired'
            ORDER BY sequence_id""".query[(String, Long)].to[Vector]
    )

  private def timerSubscriptions(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Vector[(java.util.UUID, Instant)] =
    run(
      sql"""SELECT timer_id, deadline FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[
          (java.util.UUID, Instant)
        ].to[Vector]
    )

  private def timerFiredCount(timerId: java.util.UUID): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_events
            WHERE event_kind = 'TimerFired' AND event_key = ${timerId.toString}""".query[Int].unique
    )

  test("timer sweep fires a due timer of an unattended instance and upserts a wakeup") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "ts-fire") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      s"done-$in"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      assertEquals(timerFiredEvents(wf.id), Vector.empty)

      clock.advanceBy(2.minutes)
      assertEquals(runner.runTimerSweep(), 1)

      val subId = timerSubscriptions(wf.id, "k").head._1
      assertEquals(timerFiredCount(subId), 1, "the due timer must be fired exactly once")
      assert(wakeupExists(wf.id, "k"), "the sweep must upsert the owning instance's wakeup")

      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    } finally runner.stop(1.second)
  }

  test("timer sweep leaves NOT-due subscriptions untouched") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "ts-notdue") { in =>
      Step.await[Unit]("t", Awaitable.Timer(5.minutes))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

      assertEquals(runner.runTimerSweep(), 0, "no timer is due, so nothing may fire")
      assertEquals(timerFiredEvents(wf.id), Vector.empty)
      assertEquals(timerSubscriptions(wf.id, "k").length, 1, "the subscription must survive")
    } finally runner.stop(1.second)
  }

  test("timer sweep is idempotent across passes: one event total") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "ts-idem") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      val subId = timerSubscriptions(wf.id, "k").head._1

      clock.advanceBy(2.minutes)
      assertEquals(runner.runTimerSweep(), 1)
      assertEquals(runner.runTimerSweep(), 0, "a second pass finds nothing new to fire")
      assertEquals(timerFiredCount(subId), 1, "exactly one TimerFired event total")
    } finally runner.stop(1.second)
  }

  test("timer sweep fires within a batch in deadline order") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "ts-order") { in =>
      Step.await[Unit]("t", Awaitable.Timer(in.toInt.minutes))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val ids = Vector("a" -> "1", "b" -> "2", "c" -> "3").map { case (k, delay) =>
        val id = rt.createWorkflowInstance(wf, k, delay).id
        assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
        (k, id)
      }
      clock.advanceBy(5.minutes)
      assertEquals(runner.runTimerSweep(), 3)

      val events = timerFiredEvents(wf.id)
      assertEquals(events.size, 3)
      val byKey = ids.map { case (k, id) => (k, timerSubscriptions(wf.id, k).head) }.toMap
      val orderByDeadline = ids.map(_._1).sortBy(k => byKey(k)._2)
      val orderBySequence = events.sortBy(_._2).map(e => byKey.find(_._2._1.toString == e._1).get._1)
      assertEquals(orderBySequence, orderByDeadline, "append order must follow deadline order")
    } finally runner.stop(1.second)
  }

  test("escalation sweep turns an overdue cancellation into TERMINATED") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "escalate") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      rt.cancel(id)
      assert(cancelRequestedAt(wf.id, "k").isDefined)

      clock.advanceBy(1.minute)
      assertEquals(runner.runEscalationSweep(), 1)

      assertEquals(terminalRow(wf.id, "k").get._1, Some("terminated"))
      val event = completedEvent(wf.id, "k").get
      assertEquals(
        Cacheable[WorkflowCompletionResult[String]].read(event._3),
        WorkflowCompletionResult.Terminated
      )
    } finally runner.stop(1.second)
  }

  test("escalation sweep ignores cancellations that have not yet exceeded cancelTimeout") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "escalate-pending") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      rt.cancel(id)

      clock.advanceBy(100.millis)
      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(terminalRow(wf.id, "k").get._1, None, "the instance must stay CANCELLING")
      assertEquals(completedEvent(wf.id, "k"), None)
    } finally runner.stop(1.second)
  }

  test("escalation sweep ignores terminal instances and is idempotent") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val wf = Workflow[String, String](id = "escalate-terminal") { in => s"out-$in" }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("out-in"))
      run(
        sql"""UPDATE workflow_instances SET cancel_requested_at = '2020-01-01T00:00:00Z'
              WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
      )

      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"), "a terminal instance must be untouched")

      clock.advanceBy(1.minute)
      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(completedEvent(wf.id, "k").map(_._3), Some(Cacheable[WorkflowCompletionResult[String]].write(WorkflowCompletionResult.Completed("out-in"))))
    } finally runner.stop(1.second)
  }

  test("escalation sweep is idempotent: running it again does not re-terminate") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "escalate-idem") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      rt.cancel(id)
      clock.advanceBy(1.minute)

      assertEquals(runner.runEscalationSweep(), 1)
      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(terminalRow(wf.id, "k").get._1, Some("terminated"))
      assertEquals(
        run(
          sql"""SELECT COUNT(*) FROM workflow_events
                WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = '' AND event_kind = 'WorkflowCompleted'""".query[Int].unique
        ),
        1,
        "only one WorkflowCompleted event may exist"
      )
    } finally runner.stop(1.second)
  }

  test("recovery sweep clears an expired lease, upserts a wakeup, and does not bump the fencing token") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "recover") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      val before = leaseRow(wf.id, "k").get._3

      clock.advanceBy(1.hour)
      run(
        sql"""UPDATE workflow_instances
              SET lease_owner = 'orphaned-worker', lease_expires_at = '2026-01-02T00:30:00Z'
              WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
      )

      assertEquals(runner.runRecoverySweep(), 1)

      val row = leaseRow(wf.id, "k").get
      assertEquals(row._1, None, "the expired lease owner must be cleared")
      assertEquals(row._3, before, "recovery must not bump the fencing token")
      assert(wakeupExists(wf.id, "k"), "recovery must upsert the instance's wakeup")
    } finally runner.stop(1.second)
  }

  test("recovery sweep ignores live leases and terminal instances") {
    val rt = newRuntime
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "recover-live") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      run(
        sql"""UPDATE workflow_instances
              SET lease_owner = 'live-worker', lease_expires_at = now() + interval '1 hour'
              WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
      )

      assertEquals(runner.runRecoverySweep(), 0)
      val row = leaseRow(wf.id, "k").get
      assertEquals(row._1, Some("live-worker"), "a live lease must be untouched")

      val wf2 = Workflow[String, String](id = "recover-terminal") { in => s"out-$in" }
      val id2 = rt.createWorkflowInstance(wf2, "t", "in").id
      assertEquals(rt.runWorkflowInstance(wf2, id2), WorkflowRunResult.Result("out-in"))
      run(
        sql"""UPDATE workflow_instances
              SET lease_owner = 'orphan', lease_expires_at = now() - interval '1 hour'
              WHERE workflow_id = ${wf2.id} AND workflow_instance_key = 't' AND scope = ''""".update.run
      )
      assertEquals(runner.runRecoverySweep(), 0)
      assertEquals(leaseRow(wf2.id, "t").get._1, Some("orphan"), "a terminal instance's lease must be untouched")
    } finally runner.stop(1.second)
  }

  test("cadence wiring: an unattended suspended-on-timer instance completes autonomously") {
    val rt = newRuntime
    given Clock = Clock.systemUTC()
    val wf = Workflow[String, String](id = "cadence") { in =>
      Step.await[Unit]("t", Awaitable.Timer(100.millis))
      "done"
    }
    val runner = rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndSchedule("k", "in")(using rt)
      assertEquals(inst.awaitResult(10.seconds)(using rt), WorkflowRunResult.Result("done"))
    } finally runner.stop(1.second)
  }

  test("sweeps are definition-agnostic: they service a workflow outside the runner's registry") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    given Clock = clock
    val registered = Workflow[String, String](id = "registered") { in => s"out-$in" }
    val unknown = Workflow[String, String](id = "unknown") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(registered), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(unknown, "u", "in").id
      assertEquals(rt.runWorkflowInstance(unknown, id), WorkflowRunResult.WorkflowSuspended)

      clock.advanceBy(2.minutes)
      assertEquals(runner.runTimerSweep(), 1, "the sweep fires the unknown workflow's timer")
      assertEquals(timerFiredEvents(unknown.id).size, 1)
      assert(wakeupExists(unknown.id, "u"), "the unknown workflow's wakeup is upserted")

      runner.runDriverCycle()
      assert(wakeupExists(unknown.id, "u"), "the wakeup ages unclaimed: not in this runner's registry")
    } finally runner.stop(1.second)
  }

  test("a runner-driven failure uses the settings throwableCacheable to encode its terminal outcome") {
    val rt = newRuntime
    val custom = new Cacheable[Throwable] {
      override def stableSerializedTypeId: String = "test-throwable-marker"
      override def write(value: Throwable): String = "MARKER:" + value.getClass.getName
      override def read(serialized: String): Throwable = new RuntimeException(serialized)
    }
    val wf = Workflow[String, String](id = "codec") { _ => throw new RuntimeException("boom") }
    rt.createAndSchedule(wf, "k", "x")
    val runner = rt.startJobRunnerForTests(
      Seq(wf),
      JobRunnerSettings.forTests.copy(throwableCacheable = custom)
    )
    try {
      runner.runDriverCycle()
      val outcome = terminalRow(wf.id, "k").get._2.get
      assert(outcome.contains("test-throwable-marker"), s"outcome must embed the custom codec id: $outcome")
      assert(outcome.contains("MARKER:java.lang.RuntimeException"), s"outcome must use the custom codec: $outcome")
    } finally runner.stop(1.second)
  }
}
