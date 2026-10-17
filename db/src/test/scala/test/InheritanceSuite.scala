package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.PostgresWorkflowRuntime
import doobie.implicits.*
import doobie.postgres.implicits.*

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class InheritanceSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def hasWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: Vector[String]): Boolean =
    run(
      sql"""SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".query[Int].option
    ).isDefined

  private def cursorAt(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      signalKey: SignalKey
  ): Option[Long] =
    run(
      sql"""SELECT sequence_id FROM signal_cursor
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope AND signal_key = $signalKey""".query[Long].option
    )

  private def childInfo(rt: PostgresWorkflowRuntime, parentId: WorkflowInstanceId, childKey: WorkflowInstanceKey): WorkflowInstance.Info =
    rt.getChildWorkflowInstances(parentId).find(_.id.workflowInstanceKey == childKey).get

  private def storedInheritance(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String]
  ): (Vector[String], Boolean) =
    run(
      sql"""SELECT inherited_signal_prefixes, inherit_past_events FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope"""
        .query[(Vector[String], Boolean)].unique
    )

  /** Captures the runtime logger's WARN messages emitted while `body` runs. */
  private def withCapturedWarnings[A](body: => A): (A, Vector[String]) = {
    val logger = org.slf4j.LoggerFactory
      .getLogger("atomicflow.impl.db.PostgresWorkflowRuntime")
      .asInstanceOf[ch.qos.logback.classic.Logger]
    val appender = new ch.qos.logback.core.read.ListAppender[ch.qos.logback.classic.spi.ILoggingEvent]
    appender.start()
    logger.addAppender(appender)
    try {
      val result = body
      val warnings = appender.list.asScala.toVector
        .filter(_.getLevel == ch.qos.logback.classic.Level.WARN)
        .map(_.getFormattedMessage)
      (result, warnings)
    } finally {
      logger.detachAppender(appender)
      appender.stop()
    }
  }

  test("default none: an event on the parent is invisible to the child's await") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi")
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    signal.send(parentId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.WorkflowSuspended,
      "with the default inheritance of none the child must not see the parent's event"
    )
  }

  test("all: a parent event is visible and is consumed exactly once across sequential awaits") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      val a = Step.await[String]("a1", Awaitable.SignalEvent(signal))
      val b = Step.await[String]("a2", Awaitable.SignalEvent(signal))
      s"$a|$b"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    signal.send(parentId, "go1")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)
    signal.send(parentId, "go2")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.Result("go1|go2"),
      "the first inherited event advances the child's cursor so the second await does not re-consume it"
    )
  }

  test("some(prefixes): a matching key prefix is visible; a non-matching key is not") {
    val rt = newRuntime
    val approve = Signal[String]("order/approve")
    val unrelated = Signal[String]("misc/thing")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(approve))
      "done"
    }
    val otherWf = Workflow[String, String]("worker2") { _ =>
      Step.await[String]("wait2", Awaitable.SignalEvent(unrelated))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.some(Seq("order/")))
      otherWf.startAsChild("worker-2", "hi", inheritSignals = SignalInheritance.some(Seq("order/")))
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    val otherId = childInfo(rt, parentId, "worker-2").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(otherWf, otherId), WorkflowRunResult.WorkflowSuspended)

    approve.send(parentId, "x")(using rt)
    unrelated.send(parentId, "y")(using rt)

    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.Result("done"),
      "an event whose key matches a configured prefix is inherited"
    )
    assertEquals(
      rt.runWorkflowInstance(otherWf, otherId),
      WorkflowRunResult.WorkflowSuspended,
      "an event whose key does not match any configured prefix is not inherited"
    )
  }

  private def buildChain(
      rt: PostgresWorkflowRuntime,
      gpPolicy: SignalInheritance,
      pPolicy: SignalInheritance,
      signal: Signal[String]
  ): (WorkflowInstanceId, WorkflowInstanceId) = {
    val childWf = Workflow[String, String]("tchild") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("tparent") { _ =>
      childWf.startAsChild("c", "hi", inheritSignals = pPolicy)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val grandparentWf = Workflow[String, String]("tgrand") { _ =>
      parentWf.startAsChild("p", "hi", inheritSignals = gpPolicy)
      Step.await[String]("gpwait", Awaitable.SignalEvent(Signal[String]("gpgate")))
      "done"
    }
    val gpId = rt.createWorkflowInstance(grandparentWf, "gp", "in").id
    assertEquals(rt.runWorkflowInstance(grandparentWf, gpId), WorkflowRunResult.WorkflowSuspended)
    val parentId = rt.getChildWorkflowInstances(gpId).head.id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)
    (gpId, childId)
  }

  test("transitive: all edges all -> an event on the grandparent is visible to the grandchild") {
    val rt = newRuntime
    val signal = Signal[String]("secret/key")
    val (gpId, childId) = buildChain(rt, SignalInheritance.all, SignalInheritance.all, signal)

    signal.send(gpId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(Workflow[String, String]("tchild") { _ =>
        Step.await[String]("wait", Awaitable.SignalEvent(signal))
        "done"
      }, childId),
      WorkflowRunResult.Result("done"),
      "a 3-deep chain with every edge permitting the key sees the grandparent's event"
    )
  }

  test("transitive: a narrowed middle edge hides the grandparent's event from the grandchild") {
    val rt = newRuntime
    val signal = Signal[String]("secret/key")
    val (gpId, childId) = buildChain(rt, SignalInheritance.all, SignalInheritance.some(Seq("other/")), signal)

    signal.send(gpId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(Workflow[String, String]("tchild") { _ =>
        Step.await[String]("wait", Awaitable.SignalEvent(signal))
        "done"
      }, childId),
      WorkflowRunResult.WorkflowSuspended,
      "a middle edge that does not permit the key hides the ancestor event"
    )
  }

  test("transitive: a narrowed top edge hides the grandparent's event from the grandchild") {
    val rt = newRuntime
    val signal = Signal[String]("secret/key")
    val (gpId, childId) = buildChain(rt, SignalInheritance.some(Seq("other/")), SignalInheritance.all, signal)

    signal.send(gpId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(Workflow[String, String]("tchild") { _ =>
        Step.await[String]("wait", Awaitable.SignalEvent(signal))
        "done"
      }, childId),
      WorkflowRunResult.WorkflowSuspended,
      "a top edge that does not permit the key hides the ancestor event"
    )
  }

  test("inheritPastEvents=false: an event sent before child creation is invisible; after creation it is visible") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id

    signal.send(parentId, "before")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    signal.send(parentId, "after")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("done"))
  }

  test("inheritPastEvents=true: an event sent before child creation is visible if ahead of the child's cursor") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.all, inheritPastEvents = true)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id

    signal.send(parentId, "before")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id

    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.Result("done"),
      "with inheritPastEvents the pre-creation event is visible because it is ahead of the child's cursor"
    )
  }

  test("atomic visibility: one send to the parent resumes every currently eligible suspended child") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("c1", "hi", inheritSignals = SignalInheritance.all)
      childWf.startAsChild("c2", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val c1 = childInfo(rt, parentId, "c1").id
    val c2 = childInfo(rt, parentId, "c2").id
    assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(childWf, c2), WorkflowRunResult.WorkflowSuspended)

    signal.send(parentId, "x")(using rt)

    assertEquals(
      rt.runWorkflowInstance(childWf, c1),
      WorkflowRunResult.Result("done"),
      "child 1 reads the same committed send"
    )
    assertEquals(
      rt.runWorkflowInstance(childWf, c2),
      WorkflowRunResult.Result("done"),
      "child 2 reads the same committed send"
    )
  }

  test("fixed at creation: replaying startAsChild with a wider policy keeps the stored policy and warns") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    var inherit = SignalInheritance.none
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = inherit)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    signal.send(parentId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.WorkflowSuspended,
      "under none the child does not see the parent event"
    )

    run(sql"""DELETE FROM workflow_wakeups
              WHERE workflow_id = ${childWf.id} AND workflow_instance_key = 'worker-1' AND workflow_instance_scope = ${childId.scope}""".update.run)
    assert(!hasWakeup(childWf.id, "worker-1", childId.scope), "the child's stale creation wakeup is cleared")

    inherit = SignalInheritance.all
    val (_, warnings) = withCapturedWarnings {
      rt.runWorkflowInstance(parentWf, parentId)
    }
    assertEquals(
      storedInheritance(childWf.id, "worker-1", childId.scope)._1,
      Vector.empty[String],
      "the configuration fixed at creation is kept; the replay does not replace it"
    )
    assert(
      warnings.exists(w => w.contains("different signal-inheritance configuration") && w.contains(childId.toString)),
      s"a replay that specifies a different configuration logs a WARN; got: $warnings"
    )
    assert(
      !hasWakeup(childWf.id, "worker-1", childId.scope),
      "no subtree wakeup is scheduled for a policy change anymore"
    )

    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.WorkflowSuspended,
      "the child stays under the stored none policy, so the retained parent event remains invisible"
    )
  }

  test("fixed at creation: replaying startAsChild with a narrower policy keeps the stored policy and warns") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    var inherit = SignalInheritance.all
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = inherit)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    inherit = SignalInheritance.some(Seq("other/"))
    val (_, warnings) = withCapturedWarnings {
      rt.runWorkflowInstance(parentWf, parentId)
    }
    assertEquals(
      storedInheritance(childWf.id, "worker-1", childId.scope)._1,
      Vector(""),
      "the stored all policy is kept; the narrowing replay is ignored"
    )
    assert(
      warnings.exists(w => w.contains("different signal-inheritance configuration")),
      s"the narrowing replay logs a WARN; got: $warnings"
    )

    signal.send(parentId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.Result("done"),
      "the child keeps the stored all policy, so the event remains inherited"
    )
  }

  test("a replay with an unchanged configuration is a silent no-op") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id

    val (_, warnings) = withCapturedWarnings {
      rt.runWorkflowInstance(parentWf, parentId)
    }
    assert(
      !warnings.exists(_.contains("different signal-inheritance configuration")),
      s"an unchanged configuration must not warn; got: $warnings"
    )
    assertEquals(storedInheritance(childWf.id, "worker-1", childId.scope)._1, Vector(""))
  }

  test("a diverging replay does not disturb a child's cached result") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    var inherit = SignalInheritance.all
    val childWf = Workflow[String, String]("worker") { _ =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("worker-1", "hi", inheritSignals = inherit)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id

    signal.send(parentId, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.Result("got:x"))

    inherit = SignalInheritance.some(Seq("other/"))
    val (_, warnings) = withCapturedWarnings { rt.runWorkflowInstance(parentWf, parentId) }
    assert(warnings.exists(_.contains("different signal-inheritance configuration")))
    assertEquals(storedInheritance(childWf.id, "worker-1", childId.scope)._1, Vector(""))

    signal.send(parentId, "y")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.Result("got:x"),
      "the cached step result replays unchanged despite the divergent replay"
    )
  }

  test("detachment (Abandon): inherited visibility is removed for unresolved awaits; direct signals still work") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild(
        "worker-1",
        "hi",
        inheritSignals = SignalInheritance.all,
        parentClosePolicy = ParentClosePolicy.Abandon
      )
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = childInfo(rt, parentId, "worker-1").id
    assertEquals(rt.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowSuspended)

    Signal[String]("pgate").send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("done"))

    signal.send(parentId, "x")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.WorkflowSuspended,
      "after Abandon the child no longer inherits the parent's event"
    )

    signal.send(childId, "y")(using rt)
    assertEquals(
      rt.runWorkflowInstance(childWf, childId),
      WorkflowRunResult.Result("done"),
      "a directly addressed signal to the child still resolves"
    )
  }

  test("cursor discipline: inherited consumption is local to each instance's own cursor") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("worker") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("orders") { _ =>
      childWf.startAsChild("c1", "hi", inheritSignals = SignalInheritance.all)
      childWf.startAsChild("c2", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val c1 = childInfo(rt, parentId, "c1").id
    val c2 = childInfo(rt, parentId, "c2").id
    assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(childWf, c2), WorkflowRunResult.WorkflowSuspended)

    signal.send(parentId, "x")(using rt)
    assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.Result("done"))
    assert(cursorAt(childWf.id, "c1", c1.scope, "order/approve").isDefined, "child 1's cursor advanced")

    assertEquals(
      rt.runWorkflowInstance(childWf, c2),
      WorkflowRunResult.Result("done"),
      "child 2 still consumes the same event; its cursor is independent of child 1's"
    )
    assert(cursorAt(childWf.id, "c2", c2.scope, "order/approve").isDefined, "child 2 has its own cursor")

    assertEquals(
      rt.runWorkflowInstance(parentWf, parentId),
      WorkflowRunResult.Result("done"),
      "the parent still consumes its own directly addressed event; inheritance does not touch the parent's cursor"
    )
  }
}
