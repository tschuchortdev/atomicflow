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
      sql"""
        SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].option
    ).isDefined

  private def subscriptionExists(workflowId: WorkflowId, key: WorkflowInstanceKey, signalKey: SignalKey): Boolean =
    run(
      sql"""
        SELECT 1 FROM workflow_signal_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}' AND signal_key = $signalKey
      """.query[Int].option
    ).isDefined

  private def instanceRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Int)] =
    run(
      sql"""
        SELECT terminal_state, times_executed FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[(Option[String], Int)].option
    )

  private def wakeupRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Instant, Int)] =
    run(
      sql"""
        SELECT created_at, attempts FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[(Instant, Int)].option
    )

  private def wakeupCount(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].unique
    )

  private def leaseExpiresAt(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""
        SELECT lease_expires_at FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Option[Instant]].unique
    )

  test("started runner claims the createAndScheduleForAsyncRun wakeup and completes the instance autonomously") {
    val rt = newRuntime
    val runner = rt.startJobRunner(Seq(simpleWf("auto")), JobRunnerSettings.forTests)
    try {
      val inst = simpleWf("auto").createAndScheduleForAsyncRun("k", "x")(using rt)
      assertEquals(inst.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("done-x"))
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
      val inst = wf.createAndScheduleForAsyncRun("k", "in")(using rt)
      waitUntil(subscriptionExists(wf.id, "k", "greet"))
      signal.send(inst.id, "hello")(using rt)
      assertEquals(inst.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("got:hello"))
    } finally runner.stop(1.second)
  }

  test("registry filter: a wakeup for a workflow not in the registry is never claimed") {
    val rt = newRuntime
    val wfA = simpleWf("ra")
    val wfB = simpleWf("rb")
    val runner = rt.startJobRunnerForTests(Seq(wfA), JobRunnerSettings.forTests)
    try {
      wfB.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.runDriverCycle()
      assert(wakeupExists(wfB.id, "k"), "the unknown workflow's wakeup must persist")
      assertEquals(runner.lastClaimedBatch, Vector.empty)
    } finally runner.stop(1.second)
  }

  test("the claim is FIFO by wakeup age across workflows") {
    val rt = newRuntime
    val wfA = simpleWf("fifo-a")
    val wfB = simpleWf("fifo-b")
    wfA.createAndScheduleForAsyncRun("a1", "x")(using rt)
    wfB.createAndScheduleForAsyncRun("b1", "x")(using rt)
    wfA.createAndScheduleForAsyncRun("a2", "x")(using rt)
    val runner = rt.startJobRunnerForTests(Seq(wfA, wfB), JobRunnerSettings.forTests.copy(wakeupBatchSize = 2))
    try {
      runner.runDriverCycle()
      val claimed = runner.lastClaimedBatch
      assertEquals(claimed.size, 2)
      assertEquals(claimed.map(_.workflowInstanceKey), Vector("a1", "b1"), "oldest wakeups first, regardless of workflow")
    } finally runner.stop(1.second)
  }

  test("claim deletes a wakeup only when the lease is obtained") {
    val rt = newRuntime
    val wf = simpleWf("atomic")
    wf.createAndScheduleForAsyncRun("k", "x")(using rt)
    run(
      sql"""
        UPDATE workflow_instances
        SET lease_expires_at = now() + interval '1 hour'
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.update.run
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
    rt.createAndScheduleForAsyncRun(wf, "k", "x")
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
    val bInst = bWf.createAndScheduleForAsyncRun("bk", "in")(using rt)
    val aInst = aWf.createAndScheduleForAsyncRun("ak", "in")(using rt)
    val runner = rt.startJobRunner(
      Seq(aWf, bWf),
      JobRunnerSettings.forTests.copy(pollInterval = 30.seconds)
    )
    try {
      assertEquals(aInst.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("a-done"))
      assertEquals(bInst.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("b-done"))
    } finally runner.stop(1.second)
  }

  test("multiple runners may run concurrently and stop independently; stop is idempotent") {
    val rt = newRuntime
    val wfA = simpleWf("multiA")
    val wfB = simpleWf("multiB")
    val r1 = rt.startJobRunner(Seq(wfA), JobRunnerSettings.forTests)
    val r2 = rt.startJobRunner(Seq(wfB), JobRunnerSettings.forTests)
    try {
      val a = wfA.createAndScheduleForAsyncRun("k", "a")(using rt)
      val b = wfB.createAndScheduleForAsyncRun("k", "b")(using rt)
      assertEquals(a.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("done-a"))
      assertEquals(b.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("done-b"))
    } finally {
      r1.stop(1.second)
      r1.stop(1.second)
      r2.stop(1.second)
    }
    val r3 = rt.startJobRunner(Seq(wfA), JobRunnerSettings.forTests)
    try {
      val c = wfA.createAndScheduleForAsyncRun("k2", "c")(using rt)
      assertEquals(c.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("done-c"))
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
      wf.createAndScheduleForAsyncRun("a", "a")(using rt)
      assert(aStarted.await(10, TimeUnit.SECONDS), "the in-flight run must start")
      wf.createAndScheduleForAsyncRun("b", "b")(using rt)
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

  test("a technical failure restores the wakeup with a fresh created_at, attempts+1, and releases the lease") {
    val t0 = Instant.parse("2026-01-02T00:00:00Z")
    val clock = new TestClock(t0)
    val rt = newRuntime(clock)
    val wf = simpleWf("req")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      clock.advanceBy(10.seconds)
      runner.testRunWrapper = _ => throw new RuntimeException("simulated infrastructure failure")
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch, Vector(inst.id))
      val row = wakeupRow(wf.id, "k").get
      assertEquals(row._2, 1, "the first restore must set attempts to 1")
      assertEquals(row._1, clock.instant(), "created_at must be fresh, so the instance re-enters the FIFO at the back")
      assertEquals(leaseExpiresAt(wf.id, "k"), None, "the restore must release the lease in the same transaction")
      assertEquals(instanceRow(wf.id, "k").get._1, None, "a technical failure must not terminalize the instance")
    } finally runner.stop(1.second)
  }

  test("repeated technical failures keep the instance re-executable and grow attempts") {
    val rt = newRuntime
    val wf = simpleWf("req2")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.testRunWrapper = _ => throw new RuntimeException("simulated infrastructure failure")
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._2, 1)
      runner.runDriverCycle()
      val row = wakeupRow(wf.id, "k").get
      assertEquals(row._2, 2, "the second restore must bump attempts to 2")
      assertEquals(instanceRow(wf.id, "k").get._1, None, "still re-executable: no durable failure was recorded")
    } finally runner.stop(1.second)
  }

  test("after a technical failure the next cycle runs the instance to completion") {
    val rt = newRuntime
    val wf = simpleWf("reqok")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.testRunWrapper = _ => throw new RuntimeException("simulated infrastructure failure")
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._2, 1)
      runner.testRunWrapper = run => run()
      runner.runDriverCycle()
      val row = instanceRow(wf.id, "k").get
      assertEquals(row._1, Some("completed"))
      assertEquals(row._2, 1, "the injected failure never ran the body, so it executes exactly once")
    } finally runner.stop(1.second)
  }

  test("a WorkflowTechnicalException abort is classified as technical: restored, never terminalized") {
    val rt = newRuntime
    val wf = simpleWf("wte")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.testRunWrapper = _ => throw new WorkflowTechnicalException(new RuntimeException("db down"))
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._2, 1, "the wakeup must be restored for re-execution")
      assertEquals(leaseExpiresAt(wf.id, "k"), None, "the restore must release the lease")
      assertEquals(instanceRow(wf.id, "k").get._1, None, "a technical failure must not terminalize the instance")
    } finally runner.stop(1.second)
  }

  test("a lease-lost run is not restored; the new owner is responsible") {
    val rt = newRuntime
    val wf = simpleWf("leaselost")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.testRunWrapper = { _ =>
        run(
          sql"""
            UPDATE workflow_instances SET fencing_token = fencing_token + 1,
            lease_expires_at = now() + interval '1 hour'
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
          """.update.run
        )
        throw new LeaseLostException("simulated fenced write after a takeover")
      }
      runner.runDriverCycle()
      assert(!wakeupExists(wf.id, "k"), "no wakeup may be restored when the lease was lost")
      assert(leaseExpiresAt(wf.id, "k").exists(_.isAfter(Instant.now())), "the new owner's lease must be untouched")
    } finally runner.stop(1.second)
  }

  test("a technically failing instance re-enters the FIFO at the back, behind older wakeups") {
    val rt = newRuntime
    val poison = simpleWf("poison")
    val healthy = simpleWf("poison-healthy")
    // The poison is created first, so it is initially the oldest wakeup.
    val pInst = poison.createAndScheduleForAsyncRun("p1", "x")(using rt)
    healthy.createAndScheduleForAsyncRun("h1", "x")(using rt)
    val runner = rt.startJobRunnerForTests(
      Seq(healthy, poison),
      JobRunnerSettings.forTests.copy(wakeupBatchSize = 1)
    )
    try {
      // Cycle 1 claims the oldest wakeup (p1), which fails technically and
      // re-enters the FIFO with a fresh created_at.
      runner.testRunWrapper = _ => throw new RuntimeException("poison")
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch.map(_.workflowInstanceKey), Vector("p1"))
      assertEquals(wakeupRow(poison.id, "p1").get._2, 1)
      // Cycle 2 must claim h1 — created after the poison, but before its
      // restore — so the failing instance defers to all pre-existing work.
      runner.testRunWrapper = run => run()
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch.map(_.workflowInstanceKey), Vector("h1"))
      assertEquals(instanceRow(healthy.id, "h1").get._1, Some("completed"))
      // Cycle 3 finally retries the poison, which now completes.
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch.map(_.workflowInstanceKey), Vector("p1"))
      assertEquals(instanceRow(poison.id, "p1").get._1, Some("completed"))
    } finally runner.stop(1.second)
  }

  test("a lease left behind by a failed restore is recovered by the recovery sweep") {
    // Simulates the end state of a technical failure whose restore transaction
    // could not commit: the lease is still owned and the wakeup is gone. The
    // lease-expiry recovery sweep is the backstop that re-schedules the instance.
    val rt = newRuntime
    val wf = simpleWf("restorerecover")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      run(
        sql"""
          UPDATE workflow_instances SET lease_expires_at = now() + interval '1 hour'
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        """.update.run
      )
      run(sql"DELETE FROM workflow_wakeups WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'".update.run)
      assert(!wakeupExists(wf.id, "k"))
      assert(leaseExpiresAt(wf.id, "k").isDefined, "the lease must still be recorded")
      // Expire the lease; the sweep must revoke it and re-upsert the wakeup.
      run(
        sql"""
          UPDATE workflow_instances SET lease_expires_at = now() - interval '1 hour'
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        """.update.run
      )
      runner.runRecoverySweep()
      assert(wakeupExists(wf.id, "k"), "the recovery sweep must re-schedule a lease whose restore was lost")
      assertEquals(leaseExpiresAt(wf.id, "k"), None)
      runner.runDriverCycle()
      assertEquals(instanceRow(wf.id, "k").get._1, Some("completed"))
    } finally runner.stop(1.second)
  }

  test("a wakeup for a live-leased instance does not block newer healthy wakeups") {
    val rt = newRuntime
    val busy = simpleWf("headbusy")
    val healthy = simpleWf("headhealthy")
    // The busy instance holds a live lease and has the oldest wakeup (as an
    // event-driven upsert during its run would create).
    busy.createAndScheduleForAsyncRun("b", "x")(using rt)
    run(
      sql"""
        UPDATE workflow_instances SET lease_expires_at = now() + interval '1 hour'
        WHERE workflow_id = ${busy.id} AND workflow_instance_key = 'b' AND workflow_instance_scope = '{}'
      """.update.run
    )
    healthy.createAndScheduleForAsyncRun("h", "x")(using rt)
    val runner = rt.startJobRunnerForTests(
      Seq(busy, healthy),
      JobRunnerSettings.forTests.copy(wakeupBatchSize = 1)
    )
    try {
      runner.runDriverCycle()
      assertEquals(
        runner.lastClaimedBatch.map(_.workflowInstanceKey),
        Vector("h"),
        "the live-leased head must be skipped in the candidate selection, not starve the newer healthy wakeup"
      )
      assert(wakeupExists(busy.id, "b"), "the busy instance's wakeup must stay for when its lease frees up")
    } finally runner.stop(1.second)
  }

  test("a run that fails technically after the instance turned terminal leaves no wakeup") {
    val rt = newRuntime
    val wf = simpleWf("termafter")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.testRunWrapper = { _ =>
        // Another worker terminalizes the instance while this run is failing.
        rt.terminate(WorkflowInstanceId(wf.id, "k"))
        throw new WorkflowTechnicalException(new RuntimeException("db down"))
      }
      runner.runDriverCycle()
      assertEquals(instanceRow(wf.id, "k").get._1, Some("terminated"))
      assert(!wakeupExists(wf.id, "k"), "the restore must not resurrect a wakeup for a terminal instance")
    } finally runner.stop(1.second)
  }

  test("a racing event delivery coalesces with the restored wakeup") {
    val rt = newRuntime
    val wf = simpleWf("coalesce")
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.testRunWrapper = _ => throw new WorkflowTechnicalException(new RuntimeException("db down"))
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._2, 1, "restored by the technical-failure path")
      // An event-driven delivery lands after the restore: it must not reset
      // attempts or duplicate the row.
      rt.dispatchNewEvents(1000)
      assertEquals(wakeupCount(wf.id, "k"), 1, "at most one wakeup row per instance")
      assertEquals(wakeupRow(wf.id, "k").get._2, 1, "the coalesced delivery must not reset attempts")
    } finally runner.stop(1.second)
  }

  test("an executor rejection restores the wakeup and releases the lease") {
    val rt = newRuntime
    val wf = simpleWf("rejected")
    val rejecting = new java.util.concurrent.Executor {
      def execute(r: Runnable): Unit = throw new java.util.concurrent.RejectedExecutionException("down")
    }
    val runner = rt.startJobRunnerForTests(
      Seq(wf),
      JobRunnerSettings.forTests.copy(executor = Some(rejecting))
    )
    try {
      wf.createAndScheduleForAsyncRun("k", "x")(using rt)
      runner.runDriverCycle()
      assertEquals(wakeupRow(wf.id, "k").get._2, 1, "the rejected dispatch must restore the wakeup")
      assertEquals(leaseExpiresAt(wf.id, "k"), None, "the restore must release the lease")
    } finally runner.stop(1.second)
  }

  test("recovery does not clear a lease that was renewed or taken over after expiry selection") {
    val rt = newRuntime
    val wf = simpleWf("recerenew")
    rt.createAndScheduleForAsyncRun(wf, "k", "x")
    run(
      sql"""
        UPDATE workflow_instances SET lease_expires_at = now() + interval '1 hour'
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.update.run
    )
    // The sweep would not select this lease (not expired); a racing acquisition
    // between selection and recovery is guarded by the same expiry predicate
    // inside recoverLease itself.
    assertEquals(rt.recoverLease(WorkflowInstanceId(wf.id, "k")), false)
    assert(leaseExpiresAt(wf.id, "k").exists(_.isAfter(Instant.now())), "a live lease must survive recovery")
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
        wf.createAndScheduleForAsyncRun("k", "x")(using rt)
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
