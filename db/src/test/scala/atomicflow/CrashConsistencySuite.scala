package atomicflow

import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.FaultInjector
import atomicflow.impl.db.PostgresWorkflowRuntime
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.{Clock, Instant}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** Fault injection at every durable boundary of the Postgres runtime: each
  * crashed run must leave a consistent partial state, and a later clean run must
  * resume from exactly that state rather than from scratch.
  */
class CrashConsistencySuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private final class InjectedFault(point: String)
      extends RuntimeException(s"injected fault at $point")

  private final class FailingInjector(failingPoint: String) extends FaultInjector {
    private val hits = new ConcurrentHashMap[String, AtomicInteger]

    def hitCount(point: String): Int =
      Option(hits.get(point)).fold(0)(_.get())

    private def inject(point: String): Unit = {
      hits.computeIfAbsent(point, _ => new AtomicInteger()).incrementAndGet()
      if point == failingPoint then throw new WorkflowTechnicalException(new InjectedFault(point))
    }

    override def afterStepStartedWrite(): Unit = inject("afterStepStartedWrite")
    override def beforeStepSuccessWrite(): Unit = inject("beforeStepSuccessWrite")
    override def afterStepSuccessWrite(): Unit = inject("afterStepSuccessWrite")
    override def beforeStepFailureWrite(): Unit = inject("beforeStepFailureWrite")
    override def afterStepFailureWrite(): Unit = inject("afterStepFailureWrite")

    override def afterEventAppendCommit(): Unit = inject("afterEventAppendCommit")
    override def afterTimerFireCommit(): Unit = inject("afterTimerFireCommit")
    override def afterSubscriptionRegistration(): Unit = inject("afterSubscriptionRegistration")
    override def afterWaitResolution(): Unit = inject("afterWaitResolution")

    override def afterTerminalTransition(): Unit = inject("afterTerminalTransition")
    override def afterContinueAsNew(): Unit = inject("afterContinueAsNew")
    override def afterReset(): Unit = inject("afterReset")
    override def afterChildCreate(): Unit = inject("afterChildCreate")
    override def afterChildClose(): Unit = inject("afterChildClose")

    override def afterLeaseAcquire(): Unit = inject("afterLeaseAcquire")
    override def beforeLeaseRelease(): Unit = inject("beforeLeaseRelease")
    override def afterLeaseRelease(): Unit = inject("afterLeaseRelease")
    override def afterRestoreWakeup(): Unit = inject("afterRestoreWakeup")
  }

  private def runtimeWith(injector: FaultInjector, clock: Clock = Clock.systemUTC()): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(dataSourceForTests, clock, faultInjector = injector)(using
      ExecutionContext.global
    )

  private def cleanRuntime(clock: Clock = Clock.systemUTC()): PostgresWorkflowRuntime =
    runtimeWith(FaultInjector.Noop, clock)

  private def stepState(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String,
      stepVersion: Long = 1L
  ): Option[String] =
    run(
      sql"""
        SELECT state_kind FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
          AND step_id = $stepId AND step_version = $stepVersion
      """.query[String].option
    )

  private def stepIds(workflowId: WorkflowId, key: WorkflowInstanceKey): Set[String] =
    run(
      sql"""
        SELECT step_id FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[String].to[Set]
    )

  private def stepIdsInScope(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Set[String] =
    run(
      sql"""
        SELECT step_id FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[String].to[Set]
    )

  private def stepCount(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].unique
    )

  private def terminalState(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Option[String] =
    run(
      sql"""
        SELECT terminal_state FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[Option[String]].unique
    )

  private def terminalOutcome(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[String] =
    run(
      sql"""
        SELECT terminal_outcome FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Option[String]].unique
    )

  private def generation(workflowId: WorkflowId, key: WorkflowInstanceKey): Long =
    run(
      sql"""
        SELECT generation FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Long].unique
    )

  private def storedInput(workflowId: WorkflowId, key: WorkflowInstanceKey): String =
    run(
      sql"""
        SELECT input FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[String].unique
    )

  private def instanceCount(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[Int].unique
    )

  private def wakeupCount(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[Int].unique
    )

  private def wakeupAttempts(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[Int] =
    run(
      sql"""
        SELECT attempts FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].option
    )

  private def leaseExpiresAt(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[Instant] =
    run(
      sql"""
        SELECT lease_expires_at FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Option[Instant]].unique
    )

  private def signalSubscriptionCount(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_signal_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].unique
    )

  private def timerSubscriptionCount(workflowId: WorkflowId, key: WorkflowInstanceKey): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_timer_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].unique
    )

  private def eventCount(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      kind: String
  ): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_events
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
          AND event_kind = $kind
      """.query[Int].unique
    )

  private def maxEventSequence(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      kind: String
  ): Long =
    run(
      sql"""
        SELECT COALESCE(MAX(sequence_id), 0) FROM workflow_events
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
          AND event_kind = $kind
      """.query[Long].unique
    )

  private def signalCursor(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      signalKey: SignalKey
  ): Long =
    run(
      sql"""
        SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
          AND signal_key = $signalKey
      """.query[Long].unique
    )

  private def childCloseRequestCount(): Int =
    run(sql"SELECT COUNT(*) FROM child_close_requests".query[Int].unique)

  private final case class ChildRow(
      terminalState: Option[String],
      cancelRequestedAt: Option[Instant],
      parentWorkflowId: Option[String]
  )

  private def childRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): Option[ChildRow] =
    run(
      sql"""
        SELECT terminal_state, cancel_requested_at, parent_workflow_id
        FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[(Option[String], Option[Instant], Option[String])].option
    ).map { case (state, cancel, parent) => ChildRow(state, cancel, parent) }

  private def simpleWorkflow(id: WorkflowId, counter: AtomicInteger): Workflow[String, String] =
    Workflow[String, String](id = id) { in =>
      Step.atLeastOnce[String]("step") {
        counter.incrementAndGet()
        s"done-$in"
      }
    }

  test("afterStepStartedWrite leaves a Started row, no terminal state, and a clean run executes the body again") {
    val injector = new FailingInjector("afterStepStartedWrite")
    val rt = runtimeWith(injector)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-started", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(stepState(wf.id, "k", "step"), Some("Started"))
    assertEquals(terminalState(wf.id, "k"), None)
    assertEquals(runs.get(), 0, "the body must not run after the Started crash")
    assertEquals(injector.hitCount("afterStepStartedWrite"), 1)
    assertEquals(injector.hitCount("afterStepSuccessWrite"), 0,
                 "a Started write must not fire the success hook")
    assertEquals(injector.hitCount("afterStepFailureWrite"), 0,
                 "a Started write must not fire the failure hook")

    val recorder = new FailingInjector("")
    assertEquals(runtimeWith(recorder).runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 1)
    assertEquals(stepState(wf.id, "k", "step"), Some("Succeeded"))
    assertEquals(recorder.hitCount("afterStepStartedWrite"), 1,
                 "the resumed run's one Started write fires the Started hook exactly once, so the Succeeded write fired no Started hook")
    assertEquals(recorder.hitCount("afterStepSuccessWrite"), 1)
    assertEquals(recorder.hitCount("afterStepFailureWrite"), 0,
                 "the Succeeded write must not fire the failure hook")
  }

  test("beforeStepSuccessWrite runs the body once, persists no success, and a clean run executes it again") {
    val rt = runtimeWith(FailingInjector("beforeStepSuccessWrite"))
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-before-success", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(runs.get(), 1)
    assertEquals(stepState(wf.id, "k", "step"), Some("Started"))
    assertEquals(terminalState(wf.id, "k"), None)

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 2)
    assertEquals(stepState(wf.id, "k", "step"), Some("Succeeded"))
  }

  test("afterStepSuccessWrite persists Succeeded and a clean run returns the cached value without re-executing") {
    val rt = runtimeWith(FailingInjector("afterStepSuccessWrite"))
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-after-success", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(runs.get(), 1)
    assertEquals(stepState(wf.id, "k", "step"), Some("Succeeded"))
    assertEquals(terminalState(wf.id, "k"), None)

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 1, "the clean run must replay the cached step")
  }

  test("beforeStepFailureWrite does not persist the failure and a clean run re-executes the failing body") {
    val rt = runtimeWith(FailingInjector("beforeStepFailureWrite"))
    val runs = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "crash-before-failure") { in =>
      Step.atLeastOnce[String]("step") {
        runs.incrementAndGet()
        throw new RuntimeException(s"boom-$in")
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(runs.get(), 1)
    assertEquals(stepState(wf.id, "k", "step"), Some("Started"))
    assertEquals(terminalState(wf.id, "k"), None)

    val result = cleanRuntime().runWorkflowInstance(wf, id)
    result match {
      case WorkflowRunResult.Failed(t) => assert(t.getMessage.contains("boom-in"), t.getMessage)
      case other                       => fail(s"expected a durable step failure, got $other")
    }
    assertEquals(runs.get(), 2, "the failure was not cached, so the body ran again")
    assertEquals(stepState(wf.id, "k", "step"), Some("Failed"))
    assertEquals(terminalState(wf.id, "k"), Some("failed"))
  }

  test("afterStepFailureWrite persists the failure and a clean run replays it without re-executing") {
    val rt = runtimeWith(FailingInjector("afterStepFailureWrite"))
    val runs = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "crash-after-failure") { in =>
      Step.atLeastOnce[String]("step") {
        runs.incrementAndGet()
        throw new RuntimeException(s"boom-$in")
      }
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(runs.get(), 1)
    assertEquals(stepState(wf.id, "k", "step"), Some("Failed"))
    assertEquals(terminalState(wf.id, "k"), None)

    val result = cleanRuntime().runWorkflowInstance(wf, id)
    result match {
      case WorkflowRunResult.Failed(t) => assert(t.getMessage.contains("boom-in"), t.getMessage)
      case other                       => fail(s"expected a durable step failure, got $other")
    }
    assertEquals(runs.get(), 1, "the cached failure must not re-execute the body")
    assertEquals(terminalState(wf.id, "k"), Some("failed"))
  }

  test("afterEventAppendCommit commits the signal and a clean run resolves the waiting workflow") {
    val rt = runtimeWith(FailingInjector("afterEventAppendCommit"))
    val sig = Signal[String]("go")
    val wf = Workflow[String, String](id = "crash-event-append") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"got-$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    intercept[WorkflowTechnicalException] {
      sig.send(id, "hello")(using rt)
    }

    assertEquals(eventCount(wf.id, "k", "Signal"), 1, "the signal event committed before the fault")
    assertEquals(wakeupCount(wf.id, "k"), 0, "the sender must not wake the instance")

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("got-hello"))
  }

  test("afterSubscriptionRegistration leaves the subscription without a result and a later signal plus clean run resolves") {
    val rt = runtimeWith(FailingInjector("afterSubscriptionRegistration"))
    val sig = Signal[String]("go")
    val wf = Workflow[String, String](id = "crash-subscription") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"got-$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(signalSubscriptionCount(wf.id, "k"), 1)
    assertEquals(stepState(wf.id, "k", "wait", stepVersion = 0L), None, "no await result may exist")
    assertEquals(terminalState(wf.id, "k"), None)

    val clean = cleanRuntime()
    sig.send(id, "hello")(using clean)
    assertEquals(clean.runWorkflowInstance(wf, id), WorkflowRunResult.Result("got-hello"))
  }

  test("afterSubscriptionRegistration fires only for a newly inserted subscription") {
    val injector = new FailingInjector("")
    val rt = runtimeWith(injector)
    val sig = Signal[String]("repeat")
    val wf = Workflow[String, String](id = "crash-subscription-repeat") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"got-$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(injector.hitCount("afterSubscriptionRegistration"), 1)
    assertEquals(signalSubscriptionCount(wf.id, "k"), 1)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(signalSubscriptionCount(wf.id, "k"), 1, "re-evaluation must not duplicate the subscription")
    assertEquals(injector.hitCount("afterSubscriptionRegistration"), 1,
                 "re-evaluation with an existing subscription must not fire the hook")
  }

  test("afterWaitResolution persists the await result and a clean run returns it without re-evaluating") {
    val evaluations = new AtomicInteger(0)
    val sig = Signal[String]("go")
    val wf = Workflow[String, String](id = "crash-wait-resolution") { in =>
      val v = Step.await[String](
        "wait",
        Awaitable.SignalEvent(
          sig,
          filter = _ => {
            evaluations.incrementAndGet()
            true
          }
        )
      )
      s"got-$v"
    }
    val rt = runtimeWith(FailingInjector("afterWaitResolution"))
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(signalSubscriptionCount(wf.id, "k"), 1)
    sig.send(id, "hello")(using rt)
    val signalSequence = maxEventSequence(wf.id, "k", "Signal")
    assert(signalSequence > 0L)

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(evaluations.get(), 1)
    assertEquals(stepState(wf.id, "k", "wait", stepVersion = 0L), Some("Succeeded"))
    assertEquals(terminalState(wf.id, "k"), None)
    assertEquals(signalCursor(wf.id, "k", sig.key), signalSequence,
                 "the committed resolution advances the signal cursor")
    assertEquals(signalSubscriptionCount(wf.id, "k"), 0,
                 "the committed resolution deletes the wait site's subscriptions")

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("got-hello"))
    assertEquals(evaluations.get(), 1, "the cached await result must not be re-evaluated")
  }

  test("afterTimerFireCommit commits TimerFired without resolving the timer; a clean run resolves it") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val injector = new FailingInjector("afterTimerFireCommit")
    val rt = runtimeWith(injector, clock)
    val wf = Workflow[String, String](id = "crash-timer-fire") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      s"done-$in"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    clock.advanceBy(2.minutes)
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      intercept[WorkflowTechnicalException](runner.runTimerSweep())

      assertEquals(injector.hitCount("afterTimerFireCommit"), 1)
      assertEquals(eventCount(wf.id, "k", "TimerFired"), 1)
      assertEquals(wakeupCount(wf.id, "k"), 1, "the fired timer's wakeup committed before the fault")
      assertEquals(timerSubscriptionCount(wf.id, "k"), 1, "the crashed sweep did not resolve the timer")
      assertEquals(stepState(wf.id, "k", "t", stepVersion = 0L), None)
      assertEquals(terminalState(wf.id, "k"), None)

      assertEquals(runner.runTimerSweep(), 0, "the already-fired timer is not selected again")
      assertEquals(injector.hitCount("afterTimerFireCommit"), 1,
                   "a repeated sweep must not append or hook another TimerFired")
      assertEquals(eventCount(wf.id, "k", "TimerFired"), 1)
    } finally runner.stop(1.second)

    val clean = cleanRuntime(clock)
    assertEquals(clean.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(timerSubscriptionCount(wf.id, "k"), 0)
  }

  test("await-side timer firing runs afterTimerFireCommit before resolution") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val injector = new FailingInjector("afterTimerFireCommit")
    val rt = runtimeWith(injector, clock)
    val wf = Workflow[String, String](id = "crash-await-timer-fire") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      s"done-$in"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    clock.advanceBy(2.minutes)
    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(injector.hitCount("afterTimerFireCommit"), 1)
    assertEquals(eventCount(wf.id, "k", "TimerFired"), 1)
    assertEquals(wakeupCount(wf.id, "k"), 0,
                 "await-side firing appends the event; this live run is the delivery vehicle")
    assertEquals(timerSubscriptionCount(wf.id, "k"), 1, "the fault fired before the resolution transaction")
    assertEquals(stepState(wf.id, "k", "t", stepVersion = 0L), None)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"),
                 "resuming with the same failing runtime must not re-fire the already-fired timer")
    assertEquals(injector.hitCount("afterTimerFireCommit"), 1,
                 "the already-fired timer must not fire the hook again")
    assertEquals(timerSubscriptionCount(wf.id, "k"), 0)
  }

  test("afterTerminalTransition commits terminal state and cleanup; a clean run decodes it without re-executing") {
    val rt = runtimeWith(FailingInjector("afterTerminalTransition"))
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-terminal", runs)
    val id = rt.createAndScheduleForAsyncRun(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(runs.get(), 1)
    assertEquals(terminalState(wf.id, "k"), Some("completed"))
    assert(terminalOutcome(wf.id, "k").isDefined)
    assertEquals(eventCount(wf.id, "k", "WorkflowCompleted"), 1)
    assertEquals(wakeupCount(wf.id, "k"), 0, "terminal cleanup removes the wakeup")

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 1, "the terminal instance must not re-execute its body")
  }

  test("terminate commits cleanup of a suspended instance's real signal subscription before the fault") {
    val sig = Signal[String]("terminal-cleanup")
    val wf = Workflow[String, String](id = "crash-terminal-cleanup") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"got-$v"
    }
    val rt = runtimeWith(FailingInjector("afterTerminalTransition"))
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(signalSubscriptionCount(wf.id, "k"), 1)

    intercept[WorkflowTechnicalException](rt.terminate(id))

    assertEquals(terminalState(wf.id, "k"), Some("terminated"))
    assertEquals(signalSubscriptionCount(wf.id, "k"), 0,
                 "the terminal transition removes the suspended instance's subscription")
    assertEquals(wakeupCount(wf.id, "k"), 0)

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowTerminated,
                 "a clean run must decode the persisted terminated outcome")
  }

  test("afterContinueAsNew commits the new generation and a clean run completes it") {
    val rt = runtimeWith(FailingInjector("afterContinueAsNew"))
    val wf = Workflow[String, String](id = "crash-continue-as-new") { in =>
      Step.atLeastOnce[String]("A") { in }
      if (in == "start") Workflow.continueAsNew("next")
      s"done-$in"
    }
    val id = rt.createWorkflowInstance(wf, "k", "start").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(generation(wf.id, "k"), 1L)
    assertEquals(stepCount(wf.id, "k"), 0, "the old generation's steps are erased")
    assertEquals(storedInput(wf.id, "k"), "next")
    assertEquals(wakeupCount(wf.id, "k"), 1, "the successor generation is scheduled")

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-next"))
    assertEquals(generation(wf.id, "k"), 1L)
  }

  test("afterReset commits the invalidation and a clean run replays and completes") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = runtimeWith(FailingInjector("afterReset"), clock)
    val executionsA = new AtomicInteger(0)
    val executionsB = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "crash-reset") { in =>
      Step.atLeastOnce[String]("A") {
        executionsA.incrementAndGet()
        "a"
      }
      Step.atLeastOnce[String]("B") {
        executionsB.incrementAndGet()
        "b"
      }
      Step.await[Unit]("wait", Awaitable.Timer(1.minute))
      s"done-$in"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(stepIds(wf.id, "k"), Set("A", "B"))
    assertEquals(executionsA.get(), 1)
    assertEquals(executionsB.get(), 1)

    intercept[WorkflowTechnicalException] {
      rt.resetWorkflow[String, String](id, Seq(stepId"/B"), Seq.empty)(using wf)
    }

    assertEquals(generation(wf.id, "k"), 1L)
    assertEquals(stepIds(wf.id, "k"), Set("A"), "the invalidated step is gone")
    assertEquals(wakeupCount(wf.id, "k"), 1, "the reset instance is scheduled")

    clock.advanceBy(2.minutes)
    assertEquals(cleanRuntime(clock).runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(executionsA.get(), 1, "the retained step is replayed from its cached result")
    assertEquals(executionsB.get(), 2, "the invalidated step re-executes after the reset")
  }

  test("afterChildCreate commits the child and its wakeup; a clean parent run does not duplicate it") {
    val rt = runtimeWith(FailingInjector("afterChildCreate"))
    val childWf = Workflow[String, String](id = "crash-child") { in => s"child-$in" }
    val parentWf = Workflow[String, String](id = "crash-child-parent") { in =>
      childWf.startAsChild("c", "hi")
      s"parent-$in"
    }
    val id = rt.createWorkflowInstance(parentWf, "p", "in").id
    val scope = ScopePath("crash-child-parent", "p@0")

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(parentWf, id))

    assertEquals(instanceCount(childWf.id, "c", scope), 1)
    assertEquals(wakeupCount(childWf.id, "c", scope), 1)
    assertEquals(terminalState(parentWf.id, "p"), None)

    assertEquals(cleanRuntime().runWorkflowInstance(parentWf, id), WorkflowRunResult.Result("parent-in"))
    assertEquals(instanceCount(childWf.id, "c", scope), 1, "the replayed start must not duplicate the child")
    assertEquals(wakeupCount(childWf.id, "c", scope), 1)
  }

  test("afterChildClose commits close delivery, schedules a suspended child, and a clean run cancels it") {
    val injector = new FailingInjector("afterChildClose")
    val rt = runtimeWith(injector)
    val parentGate = Signal[String]("crash-close-parent-gate")
    val childGate = Signal[String]("crash-close-child-gate")
    val childWf = Workflow[String, String](id = "crash-close-child") { in =>
      Step.await[String]("wait", Awaitable.SignalEvent(childGate))
      s"child-$in"
    }
    val parentWf = Workflow[String, String](id = "crash-close-parent") { in =>
      childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      Step.await[String]("wait", Awaitable.SignalEvent(parentGate))
      s"parent-$in"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val scope = ScopePath("crash-close-parent", "p@0")
    val childId = WorkflowInstanceId(childWf.id, "c", scope)

    val childRunner = rt.startJobRunnerForTests(Seq(childWf), JobRunnerSettings.forTests)
    try childRunner.runDriverCycle()
    finally childRunner.stop(1.second)
    assertEquals(injector.hitCount("afterChildClose"), 0, "an unmatched child must not fire the close hook")
    assertEquals(childRow(childWf.id, "c", scope).get.terminalState, None)
    assertEquals(wakeupCount(childWf.id, "c", scope), 0, "the child suspended and consumed its creation wakeup")

    parentGate.send(parentId, "go")(using rt)
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("parent-in"))
    assertEquals(childCloseRequestCount(), 1)

    val runner = rt.startJobRunnerForTests(Seq(childWf), JobRunnerSettings.forTests)
    try {
      intercept[WorkflowTechnicalException](runner.runChildCloseSweep())
    } finally runner.stop(1.second)

    assertEquals(injector.hitCount("afterChildClose"), 1)
    val afterCrash = childRow(childWf.id, "c", scope).get
    assert(afterCrash.cancelRequestedAt.isDefined, "the close was applied before the fault")
    assertEquals(afterCrash.parentWorkflowId, None, "the child was detached")
    assertEquals(afterCrash.terminalState, None, "Cancel delivery flags the child; it is not finalized in place")
    assertEquals(wakeupCount(childWf.id, "c", scope), 1, "close delivery schedules the suspended child")
    assertEquals(childCloseRequestCount(), 1, "the crashed sweep did not remove the exhausted request")

    val cleanRecorder = new FailingInjector("")
    val clean = runtimeWith(cleanRecorder)
    val cleanRunner = clean.startJobRunnerForTests(Seq(childWf), JobRunnerSettings.forTests)
    try {
      assertEquals(cleanRunner.runChildCloseSweep(), 0)
    } finally cleanRunner.stop(1.second)

    val afterCleanSweep = childRow(childWf.id, "c", scope).get
    assertEquals(afterCleanSweep.cancelRequestedAt, afterCrash.cancelRequestedAt, "a repeated sweep must not change the close")
    assertEquals(childCloseRequestCount(), 0, "the clean sweep removes the exhausted request")
    assertEquals(cleanRecorder.hitCount("afterChildClose"), 0, "an already-applied close must not fire the hook again")

    assertEquals(clean.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowCancelled)
    assertEquals(childRow(childWf.id, "c", scope).get.terminalState, Some("cancelled"))
  }

  test("run-start self-check applies a close before afterChildClose faults; a clean run cancels") {
    val injector = new FailingInjector("afterChildClose")
    val rt = runtimeWith(injector)
    val childRuns = new AtomicInteger(0)
    val childWf = Workflow[String, String](id = "crash-selfcheck-child") { in =>
      Step.atLeastOnce[String]("work") {
        childRuns.incrementAndGet()
        in.toUpperCase
      }
    }
    val parentWf = Workflow[String, String](id = "crash-selfcheck-parent") { in =>
      childWf.startAsChild("c", "hi", parentClosePolicy = ParentClosePolicy.Cancel)
      s"parent-$in"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.Result("parent-in"))
    val scope = ScopePath("crash-selfcheck-parent", "p@0")
    val childId = WorkflowInstanceId(childWf.id, "c", scope)
    assertEquals(childCloseRequestCount(), 1, "the child has not run yet")

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(childWf, childId))

    assertEquals(injector.hitCount("afterChildClose"), 1)
    val afterCrash = childRow(childWf.id, "c", scope).get
    assert(afterCrash.cancelRequestedAt.isDefined, "the self-check applied the close before the fault")
    assertEquals(afterCrash.parentWorkflowId, None, "the self-check detached the child")
    assertEquals(afterCrash.terminalState, None, "the close was applied but the body never started")
    assertEquals(childRuns.get(), 0, "the delivered close must stop the child body from starting")
    assertEquals(stepIdsInScope(childWf.id, "c", scope), Set.empty,
                 "the child body never started, so no work step row may exist")
    assertEquals(childCloseRequestCount(), 1)

    val cleanRecorder = new FailingInjector("")
    val clean = runtimeWith(cleanRecorder)
    assertEquals(clean.runWorkflowInstance(childWf, childId), WorkflowRunResult.WorkflowCancelled)
    assertEquals(cleanRecorder.hitCount("afterChildClose"), 0,
                 "an already-detached child's self-check must not fire the hook")
    assertEquals(childRow(childWf.id, "c", scope).get.terminalState, Some("cancelled"))
    assertEquals(childRuns.get(), 0, "the cancelled run must never execute the child body")
    assertEquals(stepIdsInScope(childWf.id, "c", scope), Set.empty)
  }

  test("afterLeaseAcquire leaves the lease behind; expiry recovery schedules a clean completion") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = runtimeWith(FailingInjector("afterLeaseAcquire"), clock)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-lease-acquire", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assert(leaseExpiresAt(wf.id, "k").isDefined, "a crash after acquire must leave the lease behind")
    assertEquals(terminalState(wf.id, "k"), None)
    assertEquals(runs.get(), 0)

    clock.advanceBy(6.minutes)
    val clean = cleanRuntime(clock)
    assertEquals(clean.recoverLease(id), true)
    assertEquals(leaseExpiresAt(wf.id, "k"), None, "recovery clears the expired lease")
    assertEquals(wakeupCount(wf.id, "k"), 1, "recovery re-schedules the interrupted instance")

    assertEquals(clean.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 1)
  }

  test("job-runner claim commits the lease before afterLeaseAcquire faults; recovery completes it") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val injector = new FailingInjector("afterLeaseAcquire")
    val rt = runtimeWith(injector, clock)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-runner-lease-acquire", runs)
    val id = wf.createAndScheduleForAsyncRun("k", "in")(using rt).id
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      intercept[WorkflowTechnicalException](runner.runDriverCycle())

      assertEquals(injector.hitCount("afterLeaseAcquire"), 1)
      assertEquals(wakeupCount(wf.id, "k"), 0, "the claim transaction consumed the wakeup")
      assert(leaseExpiresAt(wf.id, "k").isDefined, "the claim transaction committed the lease")
      assertEquals(terminalState(wf.id, "k"), None)
      assertEquals(runs.get(), 0, "the body never ran")
    } finally runner.stop(1.second)

    clock.advanceBy(6.minutes)
    val clean = cleanRuntime(clock)
    assertEquals(clean.recoverLease(id), true)
    assertEquals(leaseExpiresAt(wf.id, "k"), None)
    assertEquals(wakeupCount(wf.id, "k"), 1, "recovery re-enqueues the claimed instance")

    val cleanRunner = clean.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try cleanRunner.runDriverCycle()
    finally cleanRunner.stop(1.second)

    assertEquals(wakeupCount(wf.id, "k"), 0, "the recovered run's claim consumed the wakeup")
    assertEquals(terminalState(wf.id, "k"), Some("completed"))
    assertEquals(runs.get(), 1)
  }

  test("releaseLeaseIfStillOwned with a stale token clears nothing and fires no after hook") {
    val injector = new FailingInjector("afterLeaseRelease")
    val rt = runtimeWith(injector)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-stale-release", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val token = rt.runTransaction(
      rt.tryAcquireLeaseQ(wf.id, "k", ScopePath.empty, 5.minutes)
    ).getOrElse(fail("the test instance must acquire a lease"))
    run(
      sql"""
        UPDATE workflow_instances SET fencing_token = fencing_token + 1
        WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
      """.update.run
    )
    val expiry = leaseExpiresAt(wf.id, "k")
    assert(expiry.isDefined, "the acquired lease is recorded")

    rt.releaseLeaseIfStillOwned(id, token)

    assertEquals(leaseExpiresAt(wf.id, "k"), expiry, "a stale token must not clear the lease")
    assertEquals(injector.hitCount("beforeLeaseRelease"), 1)
    assertEquals(injector.hitCount("afterLeaseRelease"), 0,
                 "the guarded after hook fires only for an actual release")
  }

  test("releasing the same token twice clears nothing the second time and fires no second after hook") {
    val injector = new FailingInjector("")
    val rt = runtimeWith(injector)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-double-release", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    val token = rt.runTransaction(
      rt.tryAcquireLeaseQ(wf.id, "k", ScopePath.empty, 5.minutes)
    ).getOrElse(fail("the test instance must acquire a lease"))
    assert(leaseExpiresAt(wf.id, "k").isDefined, "the acquired lease is recorded")

    rt.releaseLeaseIfStillOwned(id, token)

    assertEquals(leaseExpiresAt(wf.id, "k"), None, "the first release cleared the lease")
    assertEquals(injector.hitCount("afterLeaseRelease"), 1)

    rt.releaseLeaseIfStillOwned(id, token)

    assertEquals(leaseExpiresAt(wf.id, "k"), None, "the second release of the same token clears nothing")
    assertEquals(injector.hitCount("beforeLeaseRelease"), 2)
    assertEquals(injector.hitCount("afterLeaseRelease"), 1,
                 "a release against an already-NULL lease updates no row and must not fire the after hook")
  }

  test("beforeLeaseRelease on a suspended instance leaves the lease; recovery permits later completion") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val injector = new FailingInjector("beforeLeaseRelease")
    val rt = runtimeWith(injector, clock)
    val sig = Signal[String]("crash-before-release-suspended-gate")
    val wf = Workflow[String, String](id = "crash-before-release-suspended") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"done-$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(injector.hitCount("beforeLeaseRelease"), 1)
    assertEquals(terminalState(wf.id, "k"), None)
    assert(leaseExpiresAt(wf.id, "k").isDefined, "the rolled-back release leaves the lease in place")

    clock.advanceBy(6.minutes)
    val clean = cleanRuntime(clock)
    assertEquals(clean.recoverLease(id), true)
    assertEquals(leaseExpiresAt(wf.id, "k"), None)
    assertEquals(wakeupCount(wf.id, "k"), 1)

    sig.send(id, "hello")(using clean)
    assertEquals(clean.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-hello"))
  }

  test("afterLeaseRelease on a suspended instance clears the lease; a later signal completes it") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val injector = new FailingInjector("afterLeaseRelease")
    val rt = runtimeWith(injector, clock)
    val sig = Signal[String]("crash-after-release-suspended-gate")
    val wf = Workflow[String, String](id = "crash-after-release-suspended") { in =>
      val v = Step.await[String]("wait", Awaitable.SignalEvent(sig))
      s"done-$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(injector.hitCount("afterLeaseRelease"), 1)
    assertEquals(terminalState(wf.id, "k"), None)
    assertEquals(leaseExpiresAt(wf.id, "k"), None, "the release transaction committed before the fault")

    val clean = cleanRuntime(clock)
    sig.send(id, "hello")(using clean)
    assertEquals(clean.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-hello"))
  }

  test("beforeLeaseRelease leaves the lease for recovery; terminal state is already correct") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = runtimeWith(FailingInjector("beforeLeaseRelease"), clock)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-before-release", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(terminalState(wf.id, "k"), Some("completed"))
    assert(leaseExpiresAt(wf.id, "k").isDefined, "the rolled-back release leaves the lease in place")
    assertEquals(runs.get(), 1)

    clock.advanceBy(6.minutes)
    val clean = cleanRuntime(clock)
    assertEquals(clean.recoverLease(id), true)
    assertEquals(leaseExpiresAt(wf.id, "k"), None)

    assertEquals(clean.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 1)
  }

  test("afterLeaseRelease leaves the lease cleared; terminal state is correct") {
    val rt = runtimeWith(FailingInjector("afterLeaseRelease"))
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-after-release", runs)
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    intercept[WorkflowTechnicalException](rt.runWorkflowInstance(wf, id))

    assertEquals(terminalState(wf.id, "k"), Some("completed"))
    assertEquals(leaseExpiresAt(wf.id, "k"), None, "the release transaction committed before the fault")
    assertEquals(runs.get(), 1)

    assertEquals(cleanRuntime().runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
    assertEquals(runs.get(), 1, "the terminal instance must not re-execute its body")
  }

  test("afterRestoreWakeup leaves the restored wakeup and a released lease after a technical failure") {
    val injector = new FailingInjector("afterRestoreWakeup")
    val rt = runtimeWith(injector)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-restore-wakeup", runs)
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      wf.createAndScheduleForAsyncRun("k", "in")(using rt)
      runner.testRunWrapper = _ =>
        throw new WorkflowTechnicalException(new RuntimeException("simulated infrastructure failure"))
      runner.runDriverCycle()

      assertEquals(wakeupCount(wf.id, "k"), 1, "the restore transaction committed the wakeup")
      assertEquals(wakeupAttempts(wf.id, "k"), Some(1))
      assertEquals(leaseExpiresAt(wf.id, "k"), None, "the restore transaction released the lease")
      assertEquals(terminalState(wf.id, "k"), None)
      assertEquals(runs.get(), 0, "the injected technical failure never ran the body")
      assertEquals(injector.hitCount("afterRestoreWakeup"), 1,
                   "the restore hook must have fired exactly once")

      runner.testRunWrapper = run => run()
      runner.runDriverCycle()
      assertEquals(terminalState(wf.id, "k"), Some("completed"))
      assertEquals(runs.get(), 1)
      assertEquals(injector.hitCount("afterRestoreWakeup"), 1,
                   "a successful run must not fire the restore hook")
    } finally runner.stop(1.second)
  }

  test("afterRestoreWakeup does not fire when the failed instance is already terminal") {
    val injector = new FailingInjector("afterRestoreWakeup")
    val rt = runtimeWith(injector)
    val runs = new AtomicInteger(0)
    val wf = simpleWorkflow("crash-restore-terminal", runs)
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = wf.createAndScheduleForAsyncRun("k", "in")(using rt).id
      runner.testRunWrapper = _ => {
        rt.terminate(id)
        throw new WorkflowTechnicalException(new RuntimeException("db down after termination"))
      }
      runner.runDriverCycle()

      assertEquals(terminalState(wf.id, "k"), Some("terminated"))
      assertEquals(wakeupCount(wf.id, "k"), 0)
      assertEquals(injector.hitCount("afterRestoreWakeup"), 0,
                   "the terminal branch restores no wakeup and must not fire the hook")
    } finally runner.stop(1.second)
  }
}
