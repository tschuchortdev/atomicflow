package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.duration.*

/** Job runner behavior: claiming with registry filter and fairness, claim-atomic
  * lease acquisition, the dispatch loop (including drain), lifecycle, and
  * terminal-outcome handling. Lives in package `atomicflow` so it can drive the
  * package-private `runDriverCycle` hook deterministically.
  */
class JobRunnerSuite extends PostgresWorkflowRuntimeSuite {

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

  private def subscriptionExists(workflowId: WorkflowId, key: WorkflowInstanceKey, signalKey: SignalKey): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND signal_key = $signalKey""".query[Int].option
    ).isDefined

  private def instanceRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Int)] =
    run(
      sql"""SELECT terminal_state, times_executed FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[(Option[String], Int)].option
    )

  private def wakeupRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Instant, Instant, Int)] =
    run(
      sql"""SELECT created_at, scheduled_at, attempts FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[(Instant, Instant, Int)].option
    )

  private def deleteWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey): Unit =
    run(sql"""DELETE FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".update.run)

  private def leaseOwner(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[String] =
    run(
      sql"""SELECT lease_owner FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[Option[String]].unique
    )

  test("started runner claims the createAndSchedule wakeup and completes the instance autonomously") {
    val rt = newRuntime
    val runner = rt.startJobRunner(Seq(simpleWf("auto")), JobRunnerSettings.forTests)
    try {
      val inst = simpleWf("auto").createAndSchedule("k", "x")(using rt)
      assertEquals(inst.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("done-x"))
    } finally runner.stop(1.second)
  }

  test("runner resumes a suspended instance when a signal is sent while it is running") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "sig") { in =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val runner = rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndSchedule("k", "in")(using rt)
      waitUntil(subscriptionExists(wf.id, "k", "greet"))
      signal.send(inst.id, "hello")(using rt)
      assertEquals(inst.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("got:hello"))
    } finally runner.stop(1.second)
  }

  test("registry filter: a wakeup for a workflow not in the registry is never claimed") {
    val rt = newRuntime
    val wfA = simpleWf("ra")
    val wfB = simpleWf("rb")
    val runner = rt.startJobRunnerForTests(Seq(wfA), JobRunnerSettings.forTests)
    try {
      wfB.createAndSchedule("k", "x")(using rt)
      runner.runDriverCycle()
      assert(wakeupExists(wfB.id, "k"), "the unknown workflow's wakeup must persist")
      assertEquals(runner.lastClaimedBatch, Vector.empty)
    } finally runner.stop(1.second)
  }

  test("the ranked claim honors perWorkflowBatchShare fairness across workflows") {
    val rt = newRuntime
    val wfA = simpleWf("fa")
    val wfB = simpleWf("fb")
    (1 to 3).foreach(i => wfA.createAndSchedule(s"a$i", "x")(using rt))
    (1 to 3).foreach(i => wfB.createAndSchedule(s"b$i", "x")(using rt))
    val runner = rt.startJobRunnerForTests(
      Seq(wfA, wfB),
      JobRunnerSettings.forTests.copy(perWorkflowBatchShare = 1, wakeupBatchSize = 2)
    )
    try {
      runner.runDriverCycle()
      val claimed = runner.lastClaimedBatch
      assertEquals(claimed.size, 2)
      assert(claimed.exists(_.workflowId == wfA.id), s"expected a workflow A instance in the batch, got $claimed")
      assert(claimed.exists(_.workflowId == wfB.id), s"expected a workflow B instance in the batch, got $claimed")
    } finally runner.stop(1.second)
  }

  test("claim deletes a wakeup only when the lease is obtained") {
    val rt = newRuntime
    val wf = simpleWf("atomic")
    wf.createAndSchedule("k", "x")(using rt)
    run(
      sql"""UPDATE workflow_instances
            SET lease_owner = 'someone-else', lease_expires_at = now() + interval '1 hour'
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
    )
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      runner.runDriverCycle()
      assert(wakeupExists(wf.id, "k"), "the wakeup must persist when the lease is held by a live owner")
      assertEquals(runner.lastClaimedBatch, Vector.empty)
    } finally runner.stop(1.second)
  }

  test("a failing workflow reaches terminal FAILED and is not rescheduled") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "fail") { _ =>
      throw new RuntimeException("boom")
    }
    rt.createAndSchedule(wf, "k", "x")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      runner.runDriverCycle()
      val row = instanceRow(wf.id, "k").get
      assertEquals(row._1, Some("failed"))
      assertEquals(row._2, 1, "a failed workflow must not be retried")
      assertEquals(wakeupExists(wf.id, "k"), false, "no wakeup may remain for a failed workflow")
    } finally runner.stop(1.second)
  }

  test("drain: a wakeup created during a run is claimed without waiting a poll interval") {
    val rt = newRuntime
    val signal = Signal[String]("go")
    val bWf = Workflow[String, String](id = "drain-b") { _ =>
      Step.await[String]("w", Awaitable.SignalEvent(signal))
      "b-done"
    }
    val bId = WorkflowInstanceId(bWf.id, "bk")
    val aWf = Workflow[String, String](id = "drain-a") { _ =>
      val ctx = summon[WorkflowContext]
      Step.atLeastOnce[Unit]("emit") {
        signal.send(bId, "go")(using ctx.runtime)
        ()
      }
      "a-done"
    }
    val bInst = bWf.createAndSchedule("bk", "in")(using rt)
    val aInst = aWf.createAndSchedule("ak", "in")(using rt)
    val runner = rt.startJobRunner(
      Seq(aWf, bWf),
      JobRunnerSettings.forTests.copy(pollInterval = 30.seconds)
    )
    try {
      assertEquals(aInst.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("a-done"))
      assertEquals(bInst.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("b-done"))
    } finally runner.stop(1.second)
  }

  test("multiple runners may run concurrently and stop independently; stop is idempotent") {
    val rt = newRuntime
    val wfA = simpleWf("multiA")
    val wfB = simpleWf("multiB")
    val r1 = rt.startJobRunner(Seq(wfA), JobRunnerSettings.forTests)
    val r2 = rt.startJobRunner(Seq(wfB), JobRunnerSettings.forTests)
    try {
      val a = wfA.createAndSchedule("k", "a")(using rt)
      val b = wfB.createAndSchedule("k", "b")(using rt)
      assertEquals(a.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("done-a"))
      assertEquals(b.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("done-b"))
    } finally {
      r1.stop(1.second)
      r1.stop(1.second)
      r2.stop(1.second)
    }
    val r3 = rt.startJobRunner(Seq(wfA), JobRunnerSettings.forTests)
    try {
      val c = wfA.createAndSchedule("k2", "c")(using rt)
      assertEquals(c.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("done-c"))
    } finally r3.stop(1.second)
  }

  test("stop(gracePeriod) waits for the in-flight run, stops claiming, and is idempotent") {
    val rt = newRuntime
    val started = new AtomicInteger(0)
    val finished = new AtomicInteger(0)
    val releaseA = new CountDownLatch(1)
    val aStarted = new CountDownLatch(1)
    val wf = Workflow[String, String](id = "grace") { in =>
      started.incrementAndGet()
      if (in == "a") {
        aStarted.countDown()
        releaseA.await()
      }
      try s"done-$in"
      finally finished.incrementAndGet()
    }
    val runner = rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndSchedule("a", "a")(using rt)
      assert(aStarted.await(10, TimeUnit.SECONDS), "the in-flight run must start")
      wf.createAndSchedule("b", "b")(using rt)
      val stopDone = new CountDownLatch(1)
      val stopThread = new Thread(() => {
        runner.stop(5.seconds)
        stopDone.countDown()
      })
      stopThread.setDaemon(true)
      stopThread.start()
      Thread.sleep(200)
      assertEquals(stopDone.getCount, 1L, "stop must block while the in-flight run is unfinished")
      releaseA.countDown()
      assert(stopDone.await(10, TimeUnit.SECONDS), "stop must return once the in-flight run finishes")
      assertEquals(started.get(), 1, "only the in-flight run may have started (B stays unclaimed)")
      assertEquals(finished.get(), 1, "the in-flight run must complete before stop returns")
      assert(wakeupExists(wf.id, "b"), "B's wakeup must remain unclaimed after stop")
      val t0 = System.nanoTime()
      runner.stop(1.second)
      assert((System.nanoTime() - t0) < 1.second.toNanos, "the second stop must return promptly")
    } finally {
      releaseA.countDown()
      runner.stop(1.second)
    }
  }

  test("a transient failure requeues the wakeup with backoff, attempts+1, and created_at preserved") {
    val t0 = Instant.parse("2026-01-02T00:00:00Z")
    val clock = new TestClock(t0)
    val rt = newRuntime(clock)
    val wf = simpleWf("req")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndSchedule("k", "x")(using rt)
      clock.advanceBy(10.seconds)
      runner.testRunWrapper = _ => throw new RuntimeException("simulated infrastructure failure")
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch, Vector(inst.id))
      val row = wakeupRow(wf.id, "k").get
      assertEquals(row._3, 1, "first requeue must set attempts to 1")
      assertEquals(row._1, t0, "created_at must be preserved across the claim->requeue cycle, not reset to now")
      assert(row._2.isAfter(clock.instant()), "scheduled_at must be pushed into the future by backoff")
    } finally runner.stop(1.second)
  }

  test("requeue preserves created_at across requeues while attempts and backoff grow") {
    val t0 = Instant.parse("2026-01-02T00:00:00Z")
    val clock = new TestClock(t0)
    val rt = newRuntime(clock)
    val wf = simpleWf("req2")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndSchedule("k", "x")(using rt)
      deleteWakeup(wf.id, "k")
      runner.requeueTransient(inst.id, 0, t0)
      val first = wakeupRow(wf.id, "k").get
      assertEquals(first._3, 1)
      assertEquals(first._1, t0)
      val firstScheduled = first._2
      clock.advanceBy(1.second)
      runner.requeueTransient(inst.id, 1, t0)
      val second = wakeupRow(wf.id, "k").get
      assertEquals(second._3, 2, "the second requeue must bump attempts to 2")
      assertEquals(second._1, t0, "created_at must be unchanged across requeues")
      assert(second._2.isAfter(firstScheduled), "backoff must grow with attempts")
    } finally runner.stop(1.second)
  }

  test("after a transient requeue the next cycle runs the instance to completion") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = simpleWf("reqok")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndSchedule("k", "x")(using rt)
      runner.testRunWrapper = _ => throw new RuntimeException("simulated infrastructure failure")
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._3, 1)
      runner.testRunWrapper = run => run()
      clock.advanceBy(10.seconds)
      runner.runDriverCycle()
      val row = instanceRow(wf.id, "k").get
      assertEquals(row._1, Some("completed"))
      assertEquals(row._2, 1, "the injected failure never ran the body, so it executes exactly once")
    } finally runner.stop(1.second)
  }

  test("a lease-lost run is not requeued; the new owner is responsible") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = simpleWf("leaselost")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndSchedule("k", "x")(using rt)
      runner.testRunWrapper = { _ =>
        run(
          sql"""UPDATE workflow_instances SET lease_owner = 'other', fencing_token = fencing_token + 1,
                lease_expires_at = now() + interval '1 hour'
                WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
        )
        throw new RuntimeException("simulated infrastructure failure")
      }
      runner.runDriverCycle()
      assert(!wakeupExists(wf.id, "k"), "no requeue may occur when the lease was lost")
    } finally runner.stop(1.second)
  }

  test("per-workflow cap defers excess due instances in place without a lease") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = simpleWf("cap")
    val settings = JobRunnerSettings.forTests.copy(
      maxConcurrentInstances = id => if (id == wf.id) 1 else Int.MaxValue,
      capacityRetryDelay = 5.seconds
    )
    val runner = rt.startJobRunnerForTests(Seq(wf), settings)
    try {
      wf.createAndSchedule("k1", "x")(using rt)
      wf.createAndSchedule("k2", "x")(using rt)
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch.size, 1, "only one instance may be claimed under the cap")
      val deferred = wakeupRow(wf.id, "k2").get
      assertEquals(deferred._3, 0, "defer must not touch attempts")
      assertEquals(deferred._2, clock.instant().plusSeconds(5), "deferred in place by capacityRetryDelay")
      assertEquals(leaseOwner(wf.id, "k2"), None, "no lease may be taken on a deferred instance")
      assertEquals(instanceRow(wf.id, "k1").get._1, Some("completed"))
      clock.advanceBy(6.seconds)
      runner.runDriverCycle()
      assertEquals(instanceRow(wf.id, "k2").get._1, Some("completed"))
    } finally runner.stop(1.second)
  }

  test("an injected failure releases its permit, so retries are not starved by the cap") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = simpleWf("caprel")
    val settings = JobRunnerSettings.forTests.copy(
      maxConcurrentInstances = id => if (id == wf.id) 1 else Int.MaxValue,
      capacityRetryDelay = 5.seconds
    )
    val runner = rt.startJobRunnerForTests(Seq(wf), settings)
    try {
      wf.createAndSchedule("k", "x")(using rt)
      runner.testRunWrapper = _ => throw new RuntimeException("boom")
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._3, 1, "transient failure requeued the wakeup")
      runner.testRunWrapper = run => run()
      clock.advanceBy(10.seconds)
      runner.runDriverCycle()
      assertEquals(instanceRow(wf.id, "k").get._1, Some("completed"))
    } finally runner.stop(1.second)
  }

  test("a burst from one workflow cannot monopolize a claim batch") {
    val rt = newRuntime
    val wfA = simpleWf("bursta")
    val wfB = simpleWf("burstb")
    (1 to 10).foreach(i => wfA.createAndSchedule(s"a$i", "x")(using rt))
    (1 to 10).foreach(i => wfB.createAndSchedule(s"b$i", "x")(using rt))
    val runner = rt.startJobRunnerForTests(
      Seq(wfA, wfB),
      JobRunnerSettings.forTests.copy(perWorkflowBatchShare = 4, wakeupBatchSize = 8)
    )
    try {
      runner.runDriverCycle()
      val claimed = runner.lastClaimedBatch
      assertEquals(claimed.size, 8)
      assertEquals(claimed.count(_.workflowId == wfA.id), 4, "workflow A must get at most its share")
      assertEquals(claimed.count(_.workflowId == wfB.id), 4, "workflow B must get at most its share")
    } finally runner.stop(1.second)
  }

  test("executor override: dispatched runs execute on the supplied executor and stop does not shut it down") {
    val rt = newRuntime
    val executor = Executors.newSingleThreadExecutor()
    val threadName = new AtomicReference[String]()
    val wf = Workflow[String, String](id = "exec") { in =>
      threadName.set(Thread.currentThread().getName)
      s"done-$in"
    }
    try {
      val runner = rt.startJobRunnerForTests(
        Seq(wf),
        JobRunnerSettings.forTests.copy(executor = Some(executor))
      )
      try {
        wf.createAndSchedule("k", "x")(using rt)
        runner.runDriverCycle()
        assertEquals(instanceRow(wf.id, "k").get._1, Some("completed"))
        val name = threadName.get()
        assert(name != null && name.startsWith("pool-"), s"run must execute on the supplied executor, got $name")
        assert(!name.equals(Thread.currentThread().getName), "run must not execute on the caller thread")
      } finally runner.stop(1.second)
      val marker = new AtomicBoolean(false)
      executor.execute(() => marker.set(true))
      waitUntil(marker.get())
    } finally executor.shutdownNow()
  }

  test("caller-thread run bypasses per-workflow caps") {
    val rt = newRuntime
    val wf = simpleWf("bypass")
    val settings = JobRunnerSettings.forTests.copy(maxConcurrentInstances = _ => 0)
    val runner = rt.startJobRunnerForTests(Seq(wf), settings)
    try {
      val inst = wf.createAndSchedule("k", "x")(using rt)
      assertEquals(rt.runWorkflowInstance(wf, inst.id), WorkflowRunResult.Result("done-x"))
      assertEquals(instanceRow(wf.id, "k").get._1, Some("completed"))
    } finally runner.stop(1.second)
  }

  test("duplicate workflow ids in the registry are rejected at start") {
    val rt = newRuntime
    val wf = simpleWf("dup")
    intercept[IllegalArgumentException] {
      rt.startJobRunner(Seq(wf, wf), JobRunnerSettings.forTests)
    }
  }

  private def simpleWf(id: WorkflowId): Workflow[String, String] =
    Workflow[String, String](id = id)(in => s"done-$in")
}
