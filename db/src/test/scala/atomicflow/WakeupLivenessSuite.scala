package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import scala.concurrent.duration.*

/** Wakeup liveness: the asynchronous wakeup jobs that replace the synchronous, sender-side wakeup creation. A
  * best-effort watermark dispatcher gives low-latency wakeups for the common case; a periodic full
  * subscription scan is the correctness guarantee that no wakeup is ever lost. Lives in package `atomicflow`
  * so it can drive the package-private `runDriverCycle` hook and the `private[atomicflow]` dispatch/scan
  * hooks deterministically.
  */
class WakeupLivenessSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def wakeupExists(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}""".query[
        Int].option
    ).isDefined

  private def wakeupCount(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Int =
    run(
      sql"""SELECT COUNT(*) FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}""".query[
        Int].unique
    )

  private def deleteWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Unit =
    run(
      sql"""DELETE FROM workflow_wakeups
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}""".update.run
    )

  private def watermark(): Long =
    run(sql"""SELECT dispatched_through FROM workflow_event_dispatch WHERE id = 1""".query[Long].unique)

  private def maxEventSeq(): Long =
    run(sql"""SELECT COALESCE(MAX(sequence_id), 0) FROM workflow_events""".query[Long].unique)


  test("best-effort dispatch wakes a suspended direct-signal subscriber after a send") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "liv-direct") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val inst = rt.createWorkflowInstance(wf, "k", "in")
    val id = inst.id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    signal.send(id, "hi")(using rt)
    assert(!wakeupExists(wf.id, "k", ScopePath.empty),
           "a send only appends the event; the sender must not wake subscribers synchronously")

    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      runner.runDriverCycle()
      assertEquals(inst.awaitResult(5.seconds)(using rt), WorkflowRunResult.Result("got:hi"))
    } finally runner.stop(1.second)
  }

  test("full scan recovers a completion await whose wakeup was lost") {
    val rt = newRuntime
    val child = Workflow[String, Int]("liv-child") { in => in.length }
    val parent = Workflow[String, String]("liv-parent") { _ =>
      val c = child.startAsChild("c", "hi")
      val r = Step.await[WorkflowCompletionResult[Int]]("await-child", c.completion)
      r match {
        case WorkflowCompletionResult.Completed(v) => s"child:$v"
        case other                                 => s"other:$other"
      }
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result(2))
    deleteWakeup(parent.id, "p", ScopePath.empty)
    assert(!wakeupExists(parent.id, "p", ScopePath.empty),
           "the parent's wakeup is gone; its completion await is otherwise unobservable")

    val settings = JobRunnerSettings.forTests.copy(
      fullSubscriptionScanInterval = 0.millis,
      maxConcurrentInstances = _ => 0
    )
    val runner = rt.startJobRunnerForTests(Seq(parent), settings)
    try {
      runner.runDriverCycle()
      assertEquals(
        watermark(),
        maxEventSeq(),
        "cycle 1 advanced the dispatcher watermark past the completion event, so a second cycle cannot dispatch it again"
      )
      deleteWakeup(parent.id, "p", ScopePath.empty)
      assert(!wakeupExists(parent.id, "p", ScopePath.empty),
             "the dispatcher already passed the completion event; only the full scan can wake the parent now")
      runner.runDriverCycle()
      assert(wakeupExists(parent.id, "p", ScopePath.empty), "the full scan must recreate the lost completion wakeup")
      assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.Result("child:2"))
    } finally runner.stop(1.second)
  }

  test("full scan recovers an inherited-signal await whose wakeup was lost") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val child = Workflow[String, String]("liv-worker") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val parent = Workflow[String, String]("liv-orders") { _ =>
      child.startAsChild("c", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the child's creation wakeup is cleared"
    )

    signal.send(parentId, "x")(using rt)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "a send only appends; no inherited wakeup is created synchronously"
    )

    val settings = JobRunnerSettings.forTests.copy(
      fullSubscriptionScanInterval = 0.millis,
      maxConcurrentInstances = _ => 0
    )
    val runner = rt.startJobRunnerForTests(Seq(child), settings)
    try {
      runner.runDriverCycle()
      assertEquals(
        watermark(),
        maxEventSeq(),
        "cycle 1 advanced the dispatcher watermark past the signal event, so a second cycle cannot dispatch it again"
      )
      deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
      assert(
        !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
        "the dispatcher already passed the event; only the full scan can wake the child now"
      )
      runner.runDriverCycle()
      assert(
        wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
        "the full scan must recreate the lost inherited-signal wakeup"
      )
      assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result("got:x"))
    } finally runner.stop(1.second)
  }

  test("full scan recovers an inherited-signal await with a some(...) prefix that contains 'some:'") {
    val rt = newRuntime
    val signal = Signal[String]("some:pay-1")
    val child = Workflow[String, String]("liv-worker-some") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val parent = Workflow[String, String]("liv-orders-some") { _ =>
      child.startAsChild("c", "hi", inheritSignals = SignalInheritance.some(Seq("some:pay")))
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the child's creation wakeup is cleared"
    )

    signal.send(parentId, "x")(using rt)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "a send only appends; no inherited wakeup is created synchronously"
    )

    val settings = JobRunnerSettings.forTests.copy(
      fullSubscriptionScanInterval = 0.millis,
      maxConcurrentInstances = _ => 0
    )
    val runner = rt.startJobRunnerForTests(Seq(child), settings)
    try {
      runner.runDriverCycle()
      assertEquals(
        watermark(),
        maxEventSeq(),
        "cycle 1 advanced the dispatcher watermark past the signal event, so a second cycle cannot dispatch it again"
      )
      deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
      assert(
        !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
        "the dispatcher already passed the event; only the full scan can wake the child now"
      )
      runner.runDriverCycle()
      assert(
        wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
        "the full scan must recreate the lost inherited-signal wakeup for a some(...) prefix containing 'some:'"
      )
      assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result("got:x"))
    } finally runner.stop(1.second)
  }

  test("full scan does not wake an unsatisfied subscription or a terminal target") {
    val rt = newRuntime
    val never = Signal[String]("never")
    val wf = Workflow[String, String](id = "liv-never") { _ =>
      Step.await[String]("w", Awaitable.SignalEvent(never))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    val done = Workflow[String, String](id = "liv-done") { in => s"out-$in" }
    val doneId = rt.createWorkflowInstance(done, "k", "in").id
    assertEquals(rt.runWorkflowInstance(done, doneId), WorkflowRunResult.Result("out-in"))

    val runner = rt.startJobRunnerForTests(
      Seq(wf, done),
      JobRunnerSettings.forTests.copy(fullSubscriptionScanInterval = 0.millis)
    )
    try {
      runner.runDriverCycle()
      assert(!wakeupExists(wf.id, "k", ScopePath.empty), "a subscription with no candidate event must not be woken")
      assert(!wakeupExists(done.id, "k", ScopePath.empty), "a terminal instance must not be woken")
    } finally runner.stop(1.second)
  }

  test("dispatch does not create a wakeup for a terminal instance") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "liv-terminal") { in => s"out-$in" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("out-in"))
    run(
      sql"""INSERT INTO workflow_signal_subscriptions (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, signal_key)
            VALUES (${wf.id}, 'k', '{}', 'w', '{}', 0, '', 'greet')""".update.run
    )
    run(
      sql"""INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, workflow_instance_key, workflow_instance_scope, event_key, payload, created_at)
            VALUES (nextval('workflow_event_sequence'), 'Signal', ${wf.id}, 'k', '{}', 'greet', 'hi', now())""".update.run
    )

    rt.dispatchNewEvents(1000)
    assert(
      !wakeupExists(wf.id, "k", ScopePath.empty),
      "the dispatcher must not create a wakeup for a terminal instance"
    )
  }

  test("one send to the parent wakes every currently eligible suspended child asynchronously") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("liv-eligible-child") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("liv-eligible-orders") { _ =>
      childWf.startAsChild("c1", "hi", inheritSignals = SignalInheritance.all)
      childWf.startAsChild("c2", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val c1 = rt.getChildWorkflowInstances(parentId).find(_.id.workflowInstanceKey == "c1").get.id
    val c2 = rt.getChildWorkflowInstances(parentId).find(_.id.workflowInstanceKey == "c2").get.id
    assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(childWf, c2), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(childWf.id, "c1", c1.scope)
    deleteWakeup(childWf.id, "c2", c2.scope)
    assert(!wakeupExists(childWf.id, "c1", c1.scope))
    assert(!wakeupExists(childWf.id, "c2", c2.scope))

    signal.send(parentId, "x")(using rt)
    assert(!wakeupExists(childWf.id, "c1", c1.scope), "the sender must not wake descendants synchronously")
    assert(!wakeupExists(childWf.id, "c2", c2.scope), "the sender must not wake descendants synchronously")

    val bystander = Workflow[String, String](id = "liv-eligible-bystander") { in => s"out-$in" }
    val runner = rt.startJobRunnerForTests(
      Seq(bystander),
      JobRunnerSettings.forTests.copy(fullSubscriptionScanInterval = 0.millis)
    )
    try {
      runner.runDriverCycle()
      assert(wakeupExists(childWf.id, "c1", c1.scope),
             "an eligible suspended child is woken by the parent send")
      assert(wakeupExists(childWf.id, "c2", c2.scope),
             "an eligible suspended child is woken by the parent send")
      assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.Result("done"))
      assertEquals(rt.runWorkflowInstance(childWf, c2), WorkflowRunResult.Result("done"))
    } finally runner.stop(1.second)
  }

  test("repeated full scans are idempotent: at most one wakeup row per instance") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "liv-idem") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    signal.send(id, "hi")(using rt)

    val bystander = Workflow[String, String](id = "liv-bystander") { in => s"out-$in" }
    val runner = rt.startJobRunnerForTests(
      Seq(bystander),
      JobRunnerSettings.forTests.copy(fullSubscriptionScanInterval = 0.millis)
    )
    try {
      runner.runDriverCycle()
      runner.runDriverCycle()
      assertEquals(wakeupCount(wf.id, "k", ScopePath.empty), 1, "repeated scans must not accumulate duplicate wakeups")
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("got:hi"))
    } finally runner.stop(1.second)
  }
}
