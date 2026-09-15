package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
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
      sql"""SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[Int].option
    ).isDefined

  private def subscriptionExists(workflowId: WorkflowId, key: WorkflowInstanceKey, signalKey: SignalKey): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = '' AND signal_key = $signalKey""".query[Int].option
    ).isDefined

  private def instanceRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Int)] =
    run(
      sql"""SELECT terminal_state, times_executed FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = ''""".query[(Option[String], Int)].option
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
            WHERE workflow_id = ${wf.id} AND key = 'k' AND scope = ''""".update.run
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

  test("double start throws while a runner is active; stop is idempotent; restart works after stop") {
    val rt = newRuntime
    val wf = simpleWf("lc")
    val r1 = rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    intercept[IllegalStateException] {
      rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    }
    r1.stop(1.second)
    r1.stop(1.second)
    val r2 = rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    r2.stop(1.second)
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
