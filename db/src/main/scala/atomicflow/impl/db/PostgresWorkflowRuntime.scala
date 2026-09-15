package atomicflow.impl.db

import atomicflow.*
import atomicflow.internal.{
  AwaitRaceCandidate,
  AwaitRaceCompletionLeaf,
  AwaitRaceDecision,
  AwaitRaceLeaf,
  AwaitRaceSignalLeaf,
  AwaitRaceTimerLeaf,
  AwaitSignalCandidate,
  AwaitTimerCandidate,
  Framing,
  ScopePath,
  StoredStep,
  WorkflowExecution
}
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

import java.time.Clock
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object PostgresWorkflowRuntime {
  def apply(ds: DataSource)(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(ds, Clock.systemUTC())

  def apply(ds: DataSource, clock: Clock)(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(ds, clock)

  def apply(
      ds: DataSource,
      clock: Clock,
      leaseDuration: FiniteDuration = 5.minutes,
      leaseAcquireTimeout: FiniteDuration = 30.seconds,
      durableRetryThreshold: FiniteDuration = 30.seconds
  )(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(ds, clock, leaseDuration, leaseAcquireTimeout, durableRetryThreshold)
}

/** PostgreSQL backend for [[WorkflowRuntime]]. Applies the Flyway migrations on
  * construction, then serves create/instance operations against the shared
  * tables.
  *
  * The execution lease is a conditional update on the instance row, fenced by a
  * monotonic `fencing_token`. The worker identity is `"processUuid:threadId"`
  * where `processUuid` is generated once per runtime instance; any
  * stable-per-process string is acceptable.
  */
class PostgresWorkflowRuntime private[atomicflow] (
    ds: DataSource,
    theClock: Clock,
    leaseDuration: FiniteDuration = 5.minutes,
    leaseAcquireTimeout: FiniteDuration = 30.seconds,
    private val durableRetryThreshold: FiniteDuration = 30.seconds
)(using ec: ExecutionContext)
    extends WorkflowRuntime {

  Flyway.configure().dataSource(ds).load().migrate()

  private val log = LoggerFactory.getLogger(getClass)

  private val xa = Transactor.fromDataSource[IO](ds, ec)

  private val processUuid: String = java.util.UUID.randomUUID().toString

  /** Well-known advisory-lock key for the global event-append protocol. */
  private val EventAppendLockKey: Long = 844810493715605001L

  /** Poll cadence of the passive `awaitResult` waiter. */
  private val AwaitResultPollIntervalMillis: Long = 50L

  private def clearInterrupt(): Unit = Thread.interrupted()

  private[atomicflow] def runSync[A](fa: ConnectionIO[A]): A = {
    clearInterrupt()
    fa.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)
  }

  private def workerId: String = s"$processUuid:${Thread.currentThread().getId}"

  private[atomicflow] def workerIdFor(role: String): String = s"$processUuid:$role"

  private[atomicflow] def leaseExpiry(now: java.time.Instant, duration: FiniteDuration): java.time.Instant =
    now.plus(java.time.Duration.ofNanos(duration.toNanos))

  override def clock: Clock = theClock

  override def createWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using cacheable: Cacheable[In]): WorkflowInstance[In, Out] = {
    val workflowId = workflow.id
    val instanceId = WorkflowInstanceId(workflowId, instanceKey)
    val serializedInput = cacheable.write(in)

    val (inserted, existing) = runSync {
      for {
        inserted <- sql"""
          INSERT INTO workflow_instances (workflow_id, key, scope, input, workflow_version_at_creation)
          VALUES ($workflowId, $instanceKey, '', $serializedInput, ${workflow.version})
          ON CONFLICT (workflow_id, key, scope) DO NOTHING
        """.update.run
        existing <- if (inserted == 0)
          sql"SELECT input FROM workflow_instances WHERE workflow_id = $workflowId AND key = $instanceKey AND scope = ''"
            .query[String]
            .option
        else Option.empty[String].pure[ConnectionIO]
      } yield (inserted, existing)
    }

    existing match {
      case Some(stored) if stored != serializedInput =>
        throw WorkflowInputConflictException(instanceId)
      case _ =>
        WorkflowInstance(workflow, instanceId)
    }
  }

  override def createWorkflowInstanceDiscardExisting[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using cacheable: Cacheable[In]): Boolean = {
    val workflowId = workflow.id
    val serializedInput = cacheable.write(in)
    val deleted = runSync {
      for {
        deleted <- sql"""
          DELETE FROM workflow_instances
          WHERE workflow_id = $workflowId AND key = $instanceKey AND scope = ''
        """.update.run
        _ <- sql"""
          INSERT INTO workflow_instances (workflow_id, key, scope, input, workflow_version_at_creation)
          VALUES ($workflowId, $instanceKey, '', $serializedInput, ${workflow.version})
        """.update.run
      } yield deleted
    }
    deleted > 0
  }

  /** Encodes a [[SignalInheritance]] configuration for the `inherit_signals`
    * column. `none`/`all` are stored as simple tokens; `some(prefixes)` as
    * `some:` followed by a JSON array of the prefixes (unambiguous for prefixes
    * containing newlines or commas).
    */
  private def encodeSignalInheritance(si: SignalInheritance): String = si match {
    case SignalInheritance.none           => "none"
    case SignalInheritance.all            => "all"
    case SignalInheritance.some(prefixes) => "some:" + upickle.default.write(prefixes.toSeq)
  }

  /** Starts a child workflow: create-if-absent a child instance under the scope
    * derived from the executing parent's identity, generation, and enclosing
    * `Workflow.scoped` path; record the parent relationship, close policy, and
    * inheritance configuration; and schedule the child's first wakeup, all in
    * one transaction. Idempotent on parent replay (`ON CONFLICT DO NOTHING`),
    * and throws [[WorkflowInputConflictException]] when the stored input
    * differs. The child body is never executed here.
    */
  override def startChild[In, Out](
      workflow: Workflow[In, Out],
      childKey: WorkflowInstanceKey,
      input: In,
      parentClosePolicy: ParentClosePolicy,
      inheritSignals: SignalInheritance,
      inheritPastEvents: Boolean,
      parentId: WorkflowInstanceId,
      parentGeneration: Long,
      enclosingScopePath: String
  )(using cacheable: Cacheable[In]): WorkflowInstance[In, Out] = {
    val childWorkflowId = workflow.id
    val serializedInput = cacheable.write(input)
    val derivedScope = ScopePath.deriveChildScope(
      parentId.scope,
      parentId.workflowId,
      parentId.workflowInstanceKey,
      parentGeneration,
      enclosingScopePath
    )
    val childInstanceId = WorkflowInstanceId(childWorkflowId, childKey, derivedScope)
    val policyStr = parentClosePolicy match {
      case ParentClosePolicy.Cancel  => "cancel"
      case ParentClosePolicy.Abandon => "abandon"
    }
    val inheritSignalsStr = encodeSignalInheritance(inheritSignals)

    val (inserted, existing) = runSync {
      for {
        maxSeq <- sql"SELECT COALESCE(MAX(sequence_id), 0) FROM workflow_events".query[Long].unique
        inserted <- sql"""
          INSERT INTO workflow_instances (workflow_id, key, scope, input, workflow_version_at_creation, generation,
            parent_workflow_id, parent_instance_key, parent_scope, parent_close_policy,
            inherit_signals, inherit_past_events, inherited_events_start_sequence_id)
          VALUES ($childWorkflowId, $childKey, $derivedScope, $serializedInput, ${workflow.version}, 0,
            ${parentId.workflowId}, ${parentId.workflowInstanceKey}, ${parentId.scope}, $policyStr,
            $inheritSignalsStr, $inheritPastEvents, $maxSeq)
          ON CONFLICT (workflow_id, key, scope) DO NOTHING
        """.update.run
        existing <- if (inserted == 0)
          sql"""SELECT input FROM workflow_instances
                WHERE workflow_id = $childWorkflowId AND key = $childKey AND scope = $derivedScope""".query[String].option
        else Option.empty[String].pure[ConnectionIO]
        _ <- if (inserted == 1)
          upsertWakeupIO(childWorkflowId, childKey, derivedScope, theClock.instant())
        else ().pure[ConnectionIO]
      } yield (inserted, existing)
    }

    existing match {
      case Some(stored) if stored != serializedInput =>
        throw WorkflowInputConflictException(childInstanceId)
      case _ =>
        WorkflowInstance(workflow, childInstanceId)
    }
  }

  /** Requests cooperative cancellation of an instance. In one transaction:
    * row-lock the instance; a missing instance throws, a terminal one is a no-op.
    * An instance that has never started and has no execution state is finalized
    * `CANCELLED` immediately (the body never runs). Otherwise `cancel_requested_at`
    * is set once (never reset) and, when there is no live lease owner, a wakeup is
    * upserted so the pending await is resumed and the frontier checkpoint throws.
    */
  override def cancel(instanceId: WorkflowInstanceId): Unit = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val now = theClock.instant()
    runSync {
      for {
        row <- sql"""SELECT terminal_state, times_executed, lease_owner, lease_expires_at
                     FROM workflow_instances
                     WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                     FOR UPDATE""".query[(Option[String], Int, Option[String], Option[java.time.Instant])].option
        _ <- row match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
          case Some((Some(_), _, _, _)) =>
            ().pure[ConnectionIO]
          case Some((None, timesExecuted, leaseOwner, leaseExpiresAt)) =>
            for {
              hasExecutionState <- hasExecutionStateIO(workflowId, key, scope)
              _ <- if (timesExecuted == 0 && !hasExecutionState)
                finalizeCancelledWithoutLease(workflowId, key, scope)
              else
                for {
                  _ <- sql"""UPDATE workflow_instances SET cancel_requested_at = $now
                             WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                               AND cancel_requested_at IS NULL""".update.run
                  _ <- if (noLiveLease(leaseOwner, leaseExpiresAt, now))
                    upsertWakeupIO(workflowId, key, scope, now)
                  else ().pure[ConnectionIO]
                } yield ()
            } yield ()
        }
      } yield ()
    }
    ()
  }

  /** Whether the instance row has no live lease owner at `now`: no owner, or an
    * owner whose lease has expired.
    */
  private def noLiveLease(
      owner: Option[String],
      expiresAt: Option[java.time.Instant],
      now: java.time.Instant
  ): Boolean =
    owner.isEmpty || expiresAt.forall(!_.isAfter(now))

  /** Whether the instance carries any durable execution state (step rows,
    * subscriptions, or a wakeup row). Used to detect an instance that has never
    * started and has nothing to deliver into.
    */
  private def hasExecutionStateIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): ConnectionIO[Boolean] =
    for {
      steps <- sql"""SELECT 1 FROM workflow_steps
                     WHERE workflow_id = $workflowId AND key = $key AND scope = $scope LIMIT 1""".query[Int].option
      signal <- sql"""SELECT 1 FROM workflow_signal_subscriptions
                      WHERE workflow_id = $workflowId AND key = $key AND scope = $scope LIMIT 1""".query[Int].option
      timer <- sql"""SELECT 1 FROM workflow_timer_subscriptions
                     WHERE workflow_id = $workflowId AND key = $key AND scope = $scope LIMIT 1""".query[Int].option
      completion <- sql"""SELECT 1 FROM workflow_completion_subscriptions
                          WHERE workflow_id = $workflowId AND key = $key AND scope = $scope LIMIT 1""".query[Int].option
      wakeup <- sql"""SELECT 1 FROM workflow_wakeups
                      WHERE workflow_id = $workflowId AND key = $key AND scope = $scope LIMIT 1""".query[Int].option
    } yield steps.isDefined || signal.isDefined || timer.isDefined || completion.isDefined || wakeup.isDefined

  /** Upserts a coalesced wakeup row due immediately, within the caller's
    * transaction.
    */
  private def upsertWakeupIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      now: java.time.Instant
  ): ConnectionIO[Unit] =
    sql"""INSERT INTO workflow_wakeups (workflow_id, key, scope, created_at, scheduled_at, attempts)
          VALUES ($workflowId, $key, $scope, $now, $now, 0)
          ON CONFLICT (workflow_id, key, scope) DO NOTHING""".update.run.map(_ => ())

  /** The guarded `CANCELLED` terminal transition for an instance that has no
    * execution lease (finalized from a cancel request before first start): the
    * guarded status update plus the `WorkflowCompleted(Cancelled)` event and the
    * completion-subscriber wakeups, in one transaction. The `Cancelled` outcome
    * carries no user code, so it is encoded directly with the runtime's framing.
    */
  private def finalizeCancelledWithoutLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): ConnectionIO[Unit] = {
    val payload = Framing.write("cancelled")
    for {
      updated <- sql"""UPDATE workflow_instances
                       SET terminal_state = 'cancelled', terminal_outcome = $payload, is_accepting_signals = false
                       WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                         AND terminal_state IS NULL""".update.run
      _ <- if (updated == 1)
        for {
          _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
          _ <- appendCompletedEvent(workflowId, key, scope, payload)
          _ <- wakeCompletionSubscribers(workflowId, key, scope)
          _ <- terminalCleanupIO(workflowId, key, scope)
          _ <- applyParentClosePoliciesIO(workflowId, key, scope)
        } yield ()
      else ().pure[ConnectionIO]
    } yield ()
  }

  /** Terminal cleanup of an instance's own scheduling and subscription rows:
    * its wakeup row and its signal/timer/completion subscriptions. Called inside
    * the terminal-transition transaction of every terminal state, so a terminal
    * instance never remains scheduled or subscribed.
    */
  private def terminalCleanupIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): ConnectionIO[Unit] =
    for {
      _ <- sql"""DELETE FROM workflow_wakeups
                 WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".update.run
      _ <- sql"""DELETE FROM workflow_signal_subscriptions
                 WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".update.run
      _ <- sql"""DELETE FROM workflow_timer_subscriptions
                 WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".update.run
      _ <- sql"""DELETE FROM workflow_completion_subscriptions
                 WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".update.run
    } yield ()

  /** Deletes the directly addressed `Signal` events of the instance at a terminal
    * transition. `TimerFired` and `WorkflowCompleted` events are retained
    * (subscribers and replays need them); consumers of the deleted `Signal`
    * events already hold cached Step results.
    */
  private def deleteDirectSignalEventsIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): ConnectionIO[Unit] =
    sql"""DELETE FROM workflow_events
          WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
            AND event_kind = 'Signal'""".update.run.map(_ => ())

  /** Applies each child's `ParentClosePolicy` when a parent reaches a terminal
    * state, in the same transaction as the parent's terminal transition. For
    * every child whose active parent pointer still points at this instance, the
    * pointer is cleared (both policies). `Cancel` additionally delivers a
    * cooperative cancellation per child state: a CREATED child (never started,
    * no execution state) is finalized `CANCELLED` immediately; a SUSPENDED or
    * RUNNING child gets `cancel_requested_at` set and, when no live lease
    * exists, a wakeup. `Abandon` clears the pointer only. Idempotent: after the
    * first application the pointers are cleared, so a re-trigger finds no
    * children, and an already-terminal child is a no-op.
    */
  private def applyParentClosePoliciesIO(
      parentWorkflowId: WorkflowId,
      parentKey: WorkflowInstanceKey,
      parentScope: String
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      children <- sql"""SELECT workflow_id, key, scope, parent_close_policy, terminal_state, times_executed, lease_owner, lease_expires_at
                        FROM workflow_instances
                        WHERE parent_workflow_id = $parentWorkflowId
                          AND parent_instance_key = $parentKey
                          AND parent_scope = $parentScope
                        FOR UPDATE""".query[
          (WorkflowId, WorkflowInstanceKey, String, Option[String], Option[String], Int, Option[String], Option[java.time.Instant])
        ].to[Vector]
      _ <- children.traverse_ { case (cwf, ckey, cscope, policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt) =>
        val clearPointer = sql"""UPDATE workflow_instances
                                 SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_scope = NULL
                                 WHERE workflow_id = $cwf AND key = $ckey AND scope = $cscope""".update.run.map(_ => ())
        val deliver = policy match {
          case Some("abandon") => ().pure[ConnectionIO]
          case _ =>
            terminal match {
              case Some(_) => ().pure[ConnectionIO]
              case None =>
                for {
                  _ <- if (timesExecuted == 0)
                    finalizeCancelledWithoutLease(cwf, ckey, cscope)
                  else
                    for {
                      _ <- sql"""UPDATE workflow_instances SET cancel_requested_at = $now
                                 WHERE workflow_id = $cwf AND key = $ckey AND scope = $cscope
                                   AND cancel_requested_at IS NULL""".update.run
                      _ <- if (noLiveLease(leaseOwner, leaseExpiresAt, now))
                        upsertWakeupIO(cwf, ckey, cscope, now)
                      else ().pure[ConnectionIO]
                    } yield ()
                } yield ()
            }
        }
        for {
          _ <- clearPointer
          _ <- deliver
        } yield ()
      }
    } yield ()
  }

  /** Force-stop an instance. In one transaction, row-locking the instance: a
    * missing instance throws [[WorkflowNotFoundException]], an already-terminal
    * one is a no-op. Otherwise a guarded terminal transition to `TERMINATED`
    * (appending the `WorkflowCompleted(Terminated)` event and waking completion
    * subscribers), a lease revocation (fencing-token bump and clearing the lease
    * owner/expiry, fencing out any running orphan), and terminal cleanup of the
    * instance's wakeup and subscription rows. No user code runs and nothing is
    * scheduled again.
    */
  override def terminate(instanceId: WorkflowInstanceId): Unit = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val exists = runSync {
      sql"""SELECT 1 FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[Int].option
    }
    if (exists.isEmpty)
      throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
    escalateTerminated(instanceId)
    ()
  }

  /** The guarded `TERMINATED` terminal transition, shared by the public
    * [[terminate]] and the cancellation-escalation sweep. Row-locks the
    * instance; a missing or already-terminal instance is a no-op (returns
    * `false`); otherwise the guarded status update plus the
    * `WorkflowCompleted(Terminated)` event, completion-subscriber wakeups,
    * lease revocation (fencing-token bump and clearing owner/expiry), and
    * terminal cleanup commit in one transaction. Idempotent: a concurrent
    * winner makes a later caller a no-op.
    */
  private[atomicflow] def escalateTerminated(instanceId: WorkflowInstanceId): Boolean = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val payload = Framing.write("terminated")
    runSync {
      for {
        terminal <- sql"""SELECT terminal_state FROM workflow_instances
                          WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                          FOR UPDATE""".query[Option[String]].option
        updated <- terminal match {
          case None => false.pure[ConnectionIO]
          case Some(Some(_)) => false.pure[ConnectionIO]
          case Some(None) =>
            for {
              u <- sql"""UPDATE workflow_instances
                         SET terminal_state = 'terminated', terminal_outcome = $payload,
                             is_accepting_signals = false,
                             lease_owner = NULL, lease_expires_at = NULL,
                             fencing_token = fencing_token + 1
                         WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                           AND terminal_state IS NULL""".update.run
              _ <- if (u == 1)
                for {
                  _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
                  _ <- appendCompletedEvent(workflowId, key, scope, payload)
                  _ <- wakeCompletionSubscribers(workflowId, key, scope)
                  _ <- terminalCleanupIO(workflowId, key, scope)
                  _ <- applyParentClosePoliciesIO(workflowId, key, scope)
                } yield ()
              else ().pure[ConnectionIO]
            } yield u == 1
        }
      } yield updated
    }
  }

  /** The timer-firing primitive shared by the scheduler sweep (Path 1) and await
    * evaluation (Path 2): lock the subscription row, re-check that no
    * `TimerFired` event exists yet, then append the event (global append mutex)
    * and upsert the owning instance's wakeup, all in the caller's transaction.
    * Returns whether it fired; a subscription that was concurrently retired or
    * already fired is a no-op. The subscription row survives firing.
    */
  private[atomicflow] def fireTimerSubscriptionIO(
      subscriptionId: java.util.UUID,
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): ConnectionIO[Boolean] = {
    val now = theClock.instant()
    for {
      locked <- sql"""SELECT deadline FROM workflow_timer_subscriptions
                      WHERE subscription_id = $subscriptionId
                      FOR UPDATE""".query[java.time.Instant].option
      fired <- locked match {
        case None => false.pure[ConnectionIO]
        case Some(_) =>
          for {
            exists <- sql"""SELECT 1 FROM workflow_events
                            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                              AND event_kind = 'TimerFired' AND event_key = ${subscriptionId.toString}""".query[Int].option
            _ <- if (exists.isEmpty)
              for {
                _ <- appendEvent(workflowId, key, scope, "TimerFired", subscriptionId.toString, "")
                _ <- upsertWakeupIO(workflowId, key, scope, now)
              } yield ()
            else ().pure[ConnectionIO]
          } yield exists.isEmpty
      }
    } yield fired
  }

  /** Lease recovery of an instance whose lease expired while still owned: in one
    * guarded transaction, clear `lease_owner`/`lease_expires_at` (without
    * touching `fencing_token` — the next acquire bumps it) and upsert the
    * instance's wakeup. Returns whether it recovered; a live or terminal lease
    * is a no-op. Idempotent.
    */
  private[atomicflow] def recoverLease(instanceId: WorkflowInstanceId): Boolean = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val now = theClock.instant()
    runSync {
      for {
        updated <- sql"""UPDATE workflow_instances
                         SET lease_owner = NULL, lease_expires_at = NULL
                         WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                           AND lease_owner IS NOT NULL AND terminal_state IS NULL""".update.run
        _ <- if (updated == 1) upsertWakeupIO(workflowId, key, scope, now)
             else ().pure[ConnectionIO]
      } yield updated == 1
    }
  }

  override def runWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] =
    runInstanceInternal(workflow, instanceId, leaseDuration, leaseAcquireTimeout, preAcquired = None)
      .asInstanceOf[WorkflowRunResult[Out]]

  /** Runs an instance whose lease is already held by the job runner's claim
    * (worker + fencing token supplied), skipping acquisition. Uses the same
    * body-execution code path as the public [[runWorkflowInstance]]; the runner's
    * lease settings override the runtime's defaults for this run.
    */
  private[atomicflow] def runClaimedInstance(
      workflow: Workflow[?, ?],
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long,
      leaseDuration: FiniteDuration,
      leaseAcquireTimeout: FiniteDuration
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[?] =
    runInstanceInternal(workflow, instanceId, leaseDuration, leaseAcquireTimeout, preAcquired = Some((worker, token)))

  /** The shared body-execution code path behind the public run API and the job
    * runner. Decodes the persisted input with the definition's `Cacheable[In]`,
    * reads `workflowVersionAtCreation` into the context, and executes the body
    * under the given lease (either freshly acquired or pre-acquired by a claim).
    */
  private def runInstanceInternal(
      workflow: Workflow[?, ?],
      instanceId: WorkflowInstanceId,
      runLeaseDuration: FiniteDuration,
      runLeaseAcquireTimeout: FiniteDuration,
      preAcquired: Option[(String, Long)]
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[?] =
    workflow match {
      case wf: Workflow[i, o] =>
        val outCacheable: Cacheable[o] = wf.outputCacheable
        given Cacheable[o] = outCacheable
        val completionCodec = summon[Cacheable[WorkflowCompletionResult[o]]]
        val workflowId = instanceId.workflowId
        val key = instanceId.workflowInstanceKey
        val scope = instanceId.scope

        clearInterrupt()

        val row = runSync {
          sql"""SELECT terminal_state, terminal_outcome, input, workflow_version_at_creation, generation
                FROM workflow_instances
                WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[
              (Option[String], Option[String], String, Long, Long)
            ].option
        }

        val (terminalState, terminalOutcome, inputSerialized, versionAtCreation, generation) = row match {
          case Some(r) => r
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
        }

        terminalState match {
          case Some(state) =>
            handleTerminalRead(state, terminalOutcome, outCacheable, completionCodec, instanceId)
          case None =>
            val (worker, token) = preAcquired.getOrElse {
              val w = workerId
              val t = acquireLease(workflowId, key, scope, w, runLeaseDuration, runLeaseAcquireTimeout) match {
                case Some(t) => t
                case None    => throw LeaseUnavailableException(instanceId)
              }
              (w, t)
            }

            try {
              val now = theClock.instant()
              val bumped = runSync {
                sql"""UPDATE workflow_instances
                      SET times_executed = times_executed + 1, last_run_at = $now
                      WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                        AND lease_owner = $worker AND fencing_token = $token""".update.run
              }
              if (bumped != 1) throw LeaseLostException(instanceId)

              val input = wf.inputCacheable.read(inputSerialized)
              val execution = new PostgresExecution(worker, token, workflowId, key, scope, generation, runLeaseDuration)
              val ctxInstanceId = instanceId
              val ctxVersionAtCreation = versionAtCreation
              val ctxRuntime: WorkflowRuntime = this
              val ctxExecution = execution
              val ctx = new WorkflowContext {
                override def instanceId: WorkflowInstanceId = ctxInstanceId
                override def versionAtCreation: Long = ctxVersionAtCreation
                override def runtime: WorkflowRuntime = ctxRuntime
                private[atomicflow] override def execution: WorkflowExecution = ctxExecution
              }

              log.debug(s"Running workflow instance $instanceId (fencingToken=$token)")

              var outValue: o = null.asInstanceOf[o]
              var suspended = false
              try {
                outValue = wf.body(input)(using ctx)
              } catch {
                case _: WorkflowSuspendedException =>
                  suspended = true
                case _: LeaseLostException =>
                  throw new LeaseLostException(s"Workflow instance lease lost during run: $instanceId")
                case _: WorkflowCancelledException =>
                  runUnconsumedSignals(wf, workflowId, key, scope, worker, token)
                  val payload = Framing.write("cancelled")
                  val updated = terminalTransitionAndEvent(workflowId, key, scope, worker, token, "cancelled", payload)
                  if (updated == 1) {
                    log.info(s"Workflow instance $instanceId cancelled")
                    return WorkflowRunResult.WorkflowCancelled
                  } else {
                    log.info(s"Workflow instance $instanceId cancelled; adopting the winner's terminal outcome")
                    return readTerminalAndReturn(workflowId, key, scope, outCacheable, completionCodec, instanceId)
                  }
                case WorkflowNonFatal(t) =>
                  runUnconsumedSignals(wf, workflowId, key, scope, worker, token)
                  val payload = completionCodec.write(WorkflowCompletionResult.Failed(t))
                  val updated = terminalTransitionAndEvent(workflowId, key, scope, worker, token, "failed", payload)
                  if (updated == 1) {
                    log.info(s"Workflow instance $instanceId failed", t)
                    throw cacheableThrowable.read(cacheableThrowable.write(t))
                  } else {
                    log.info(s"Workflow instance $instanceId failed; adopting the winner's terminal outcome", t)
                    return readTerminalAndReturn(workflowId, key, scope, outCacheable, completionCodec, instanceId)
                  }
              }

              if (suspended) {
                log.debug(s"Workflow instance $instanceId suspended")
                WorkflowRunResult.WorkflowSuspended
              } else {
                runUnconsumedSignals(wf, workflowId, key, scope, worker, token)
                val payload = completionCodec.write(WorkflowCompletionResult.Completed(outValue))
                val updated = terminalTransitionAndEvent(workflowId, key, scope, worker, token, "completed", payload)
                if (updated == 1) {
                  log.debug(s"Workflow instance $instanceId completed")
                  WorkflowRunResult.Result(outCacheable.read(outCacheable.write(outValue)))
                } else {
                  readTerminalAndReturn(workflowId, key, scope, outCacheable, completionCodec, instanceId)
                }
              }
            } finally {
              releaseLease(workflowId, key, scope, worker, token)
            }
        }
    }

  /** One conditional lease-acquire attempt as a `ConnectionIO`, so a caller may
    * run it inside its own transaction (the job runner's claim). Returns the new
    * fencing token on success, `None` if the lease is held by a live owner or the
    * instance is terminal. The acquire and the fencing-token read-back are a
    * single `RETURNING` statement.
    */
  private[atomicflow] def tryAcquireOnceIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      leaseDuration: FiniteDuration
  ): ConnectionIO[Option[Long]] = {
    val now = theClock.instant()
    val expires = leaseExpiry(now, leaseDuration)
    sql"""UPDATE workflow_instances
          SET lease_owner = $worker, fencing_token = fencing_token + 1, lease_expires_at = $expires
          WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
            AND terminal_state IS NULL
            AND (lease_owner IS NULL OR lease_expires_at <= $now)
          RETURNING fencing_token""".query[Long].option
  }

  /** Bounded poll of the conditional lease acquire, every 100ms up to
    * `leaseAcquireTimeout`.
    */
  private def acquireLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      leaseDuration: FiniteDuration,
      leaseAcquireTimeout: FiniteDuration
  ): Option[Long] = {
    val pollIntervalMillis = 100L
    val deadlineNanos = System.nanoTime() + leaseAcquireTimeout.toNanos
    var acquired: Option[Long] = None
    while (acquired.isEmpty && System.nanoTime() < deadlineNanos) {
      acquired = runSync(tryAcquireOnceIO(workflowId, key, scope, worker, leaseDuration))
      if (acquired.isEmpty && System.nanoTime() < deadlineNanos) Thread.sleep(pollIntervalMillis)
    }
    acquired
  }

  /** Optimization: release the lease if still ours. Correctness relies on expiry
    * plus fencing, not on this.
    */
  private def releaseLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      token: Long
  ): Unit =
    runSync {
      sql"""UPDATE workflow_instances
            SET lease_owner = NULL, lease_expires_at = NULL
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND lease_owner = $worker AND fencing_token = $token""".update.run
    }

  /** Runner-side lease release: clears the lease if it is still held by `worker`
    * at `token`. The runner calls this in a `finally` so a fault-injected abort
    * that never reached the run's own release cannot leak a lease.
    */
  private[atomicflow] def releaseLeaseIfOurs(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Unit =
    releaseLease(instanceId.workflowId, instanceId.workflowInstanceKey, instanceId.scope, worker, token)

  /** The guarded terminal transition, atomic with the `WorkflowCompleted` event
    * append and the deletion of this instance's directly addressed `Signal`
    * events: the guarded `terminal_state`/`terminal_outcome` update, the event
    * append, the subscriber wakeups, and the `Signal` deletion commit together in
    * one transaction. Wins only if the instance is not already terminal and the
    * lease is still ours. `TimerFired` and `WorkflowCompleted` events are
    * retained (subscribers and replays need them); consumers of the deleted
    * `Signal` events already hold cached Step results.
    *
    * Returns rows updated (1 if this writer won the transition, 0 if another
    * writer already made it terminal).
    */
  private def terminalTransitionAndEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      token: Long,
      state: String,
      payload: String
  ): Int =
    runSync {
      for {
        updated <- sql"""UPDATE workflow_instances
              SET terminal_state = $state, terminal_outcome = $payload, is_accepting_signals = false
              WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                AND terminal_state IS NULL AND lease_owner = $worker AND fencing_token = $token""".update.run
        _ <- if (updated == 1)
          for {
            _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
            _ <- appendCompletedEvent(workflowId, key, scope, payload)
            _ <- wakeCompletionSubscribers(workflowId, key, scope)
            _ <- terminalCleanupIO(workflowId, key, scope)
            _ <- applyParentClosePoliciesIO(workflowId, key, scope)
          } yield ()
        else ().pure[ConnectionIO]
      } yield updated
    }

  /** Flips the instance to not-accepting-signals, fenced by the completing run's
    * lease. Idempotent, so re-running it on a replay before the terminal commit
    * is safe. A signal arriving after this point is rejected by `sendSignal`.
    */
  private def setNotAcceptingSignals(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      token: Long
  ): Unit =
    runSync {
      sql"""UPDATE workflow_instances SET is_accepting_signals = false
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND lease_owner = $worker AND fencing_token = $token""".update.run
    }

  /** Gathers the currently visible unconsumed `Signal` events of the instance:
    * for every key, the events after that key's shared cursor (cursor absent
    * means from the first event), in `sequenceId` order, as their raw serialized
    * payloads. Keys with no events after their cursor are absent from the result.
    */
  private def gatherUnconsumedSignals(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): Map[SignalKey, Seq[String]] =
    runSync {
      sql"""SELECT event_key, payload FROM workflow_events
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND event_kind = 'Signal'
              AND sequence_id > (
                SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                  AND signal_key = workflow_events.event_key
              )
            ORDER BY sequence_id""".query[(String, String)].to[Vector]
    }.groupMap(_._1)(_._2)

  /** Runs the completing instance's `onUnconsumedSignals` handler as the final
    * step before the terminal transition. First flips `is_accepting_signals` to
    * false (fenced by our lease), then gathers the visible unconsumed signals,
    * then invokes the handler with the map. Runs on every terminal transition
    * driven by the completing run (normal completion and body failure).
    *
    * At-least-once: if the process crashes between the handler and the terminal
    * commit, the next run re-executes it; both the flag flip and the signal
    * gather are idempotent.
    */
  private def runUnconsumedSignals(
      workflow: Workflow[?, ?],
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      token: Long
  ): Unit = {
    setNotAcceptingSignals(workflowId, key, scope, worker, token)
    val unconsumed = gatherUnconsumedSignals(workflowId, key, scope)
    workflow.onUnconsumedSignals(unconsumed)
  }

  /** Upserts the wakeup of every instance holding a pending completion
    * subscription on the instance that just completed, `ON CONFLICT DO NOTHING`
    * with `scheduled_at = now`. Rides in the same transaction as the guarded
    * terminal update and the `WorkflowCompleted` append.
    */
  private def wakeCompletionSubscribers(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      subscribers <- sql"""SELECT workflow_id, key, scope FROM workflow_completion_subscriptions
            WHERE completed_workflow_id = $workflowId AND completed_key = $key AND completed_scope = $scope""".query[
          (WorkflowId, WorkflowInstanceKey, String)
        ].to[Vector]
      _ <- subscribers.traverse_ { case (subWf, subKey, subScope) =>
        sql"""
          INSERT INTO workflow_wakeups (workflow_id, key, scope, created_at, scheduled_at, attempts)
          VALUES ($subWf, $subKey, $subScope, $now, $now, 0)
          ON CONFLICT (workflow_id, key, scope) DO NOTHING
        """.update.run.map(_ => ())
      }
    } yield ()
  }

  /** Appends a `WorkflowCompleted` event via the global event-append protocol.
    */
  private def appendCompletedEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      payload: String
  ): ConnectionIO[Unit] =
    appendEvent(workflowId, key, scope, "WorkflowCompleted", "", payload)

  /** Appends one event via the global event-append protocol:
    * `pg_advisory_xact_lock` + `nextval('workflow_event_sequence')` + insert, all
    * within the caller's transaction and held until commit. `createdAt` comes
    * from the runtime clock.
    */
  private def appendEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      kind: String,
      eventKey: String,
      payload: String
  ): ConnectionIO[Unit] = {
    val createdAt = theClock.instant()
    for {
      _ <- takeEventAppendLock
      sequenceId <- sql"SELECT nextval('workflow_event_sequence')".query[Long].unique
      _ <- sql"""
        INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, key, scope, event_key, payload, created_at)
        VALUES ($sequenceId, $kind, $workflowId, $key, $scope, $eventKey, $payload, $createdAt)
      """.update.run
    } yield ()
  }

  /** Takes the global event-append advisory lock, transaction-scoped (released at
    * commit/rollback). `pg_advisory_xact_lock` returns `void` and a SELECT result
    * set, so it is executed via `Statement.execute` rather than `executeUpdate`.
    */
  private def takeEventAppendLock: ConnectionIO[Unit] =
    doobie.free.connection.raw { c =>
      val st = c.createStatement()
      try st.execute(s"SELECT pg_advisory_xact_lock($EventAppendLockKey)")
      finally st.close()
      ()
    }

  /** Appends a `Signal` event addressed to `workflowInstanceId`. In one
    * transaction: row-lock the instance (`SELECT ... FOR UPDATE`), return
    * [[WorkflowNotFoundException]] if absent or [[SignalSendResult.InstanceAlreadyCompleted]]
    * if it has stopped accepting signals, else append the event (payload from the
    * signal's [[Cacheable]]) and upsert a wakeup when a matching pending
    * subscription exists. Does not acquire the execution lease.
    */
  override def sendSignal[A: Cacheable](
      workflowInstanceId: WorkflowInstanceId,
      key: SignalKey,
      value: A
  ): SignalSendResult = {
    val workflowId = workflowInstanceId.workflowId
    val instanceKey = workflowInstanceId.workflowInstanceKey
    val scope = workflowInstanceId.scope
    val cacheable = summon[Cacheable[A]]
    val payload = cacheable.write(value)
    runSync {
      for {
        accepting <- sql"""SELECT is_accepting_signals FROM workflow_instances
              WHERE workflow_id = $workflowId AND key = $instanceKey AND scope = $scope
              FOR UPDATE""".query[Boolean].option
        result <- accepting match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $workflowInstanceId")
          case Some(false) => SignalSendResult.InstanceAlreadyCompleted.pure[ConnectionIO]
          case Some(true) =>
            for {
              _ <- appendEvent(workflowId, instanceKey, scope, "Signal", key, payload)
              _ <- wakeMatchingSubscribers(workflowId, instanceKey, scope, key)
            } yield SignalSendResult.Success
        }
      } yield result
    }
  }

  /** Upserts the instance's wakeup row when a pending subscription for the exact
    * signal key exists. The sender never evaluates payload filters; only
    * key-matching subscriptions matter.
    */
  private def wakeMatchingSubscribers(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      signalKey: SignalKey
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      has <- sql"""SELECT 1 FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope AND signal_key = $signalKey
            LIMIT 1""".query[Int].option
      _ <- if (has.isDefined)
        sql"""
          INSERT INTO workflow_wakeups (workflow_id, key, scope, created_at, scheduled_at, attempts)
          VALUES ($workflowId, $key, $scope, $now, $now, 0)
          ON CONFLICT (workflow_id, key, scope) DO NOTHING
        """.update.run.map(_ => ())
      else ().pure[ConnectionIO]
    } yield ()
  }

  private def readTerminalAndReturn[Out](
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      outCacheable: Cacheable[Out],
      completionCodec: Cacheable[WorkflowCompletionResult[Out]],
      instanceId: WorkflowInstanceId
  ): WorkflowRunResult[Out] = {
    val (state, outcome) = runSync {
      sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[(String, Option[String])].unique
    }
    handleTerminalRead(state, outcome, outCacheable, completionCodec, instanceId)
  }

  /** Decodes a terminal instance's stored outcome and returns/thrown it, without
    * executing the body.
    */
  private def handleTerminalRead[Out](
      state: String,
      outcome: Option[String],
      outCacheable: Cacheable[Out],
      completionCodec: Cacheable[WorkflowCompletionResult[Out]],
      instanceId: WorkflowInstanceId
  ): WorkflowRunResult[Out] = {
    def decode(): WorkflowCompletionResult[Out] = {
      val payload = outcome.getOrElse(
        throw new StepSerializationFailed(s"Terminal instance $instanceId has no terminal outcome")
      )
      try completionCodec.read(payload)
      catch {
        case _: Throwable =>
          throw new StepSerializationFailed(s"Terminal outcome of $instanceId could not be decoded")
      }
    }

    state match {
      case "completed" =>
        decode() match {
          case WorkflowCompletionResult.Completed(v) => WorkflowRunResult.Result(v)
          case other =>
            throw new StepSerializationFailed(s"Terminal instance $instanceId stored a non-completed outcome as completed")
        }
      case "failed" =>
        decode() match {
          case WorkflowCompletionResult.Failed(t) => throw t
          case other =>
            throw new StepSerializationFailed(s"Terminal instance $instanceId stored a non-failed outcome as failed")
        }
      case "cancelled" => WorkflowRunResult.WorkflowCancelled
      case "terminated" => WorkflowRunResult.WorkflowTerminated
      case other =>
        throw new StepSerializationFailed(s"Unknown terminal state '$other' for instance $instanceId")
    }
  }

  private[atomicflow] override def readStep(
      instanceId: WorkflowInstanceId,
      stepId: StepId,
      stepVersion: Long
  ): Option[StoredStep] =
    readStepRow(
      instanceId.workflowId,
      instanceId.workflowInstanceKey,
      instanceId.scope,
      stepId.key,
      stepId.scope,
      stepVersion
    )

  private def readStepRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      stepId: String,
      stepScopePath: String,
      stepVersion: Long
  ): Option[StoredStep] =
    runSync {
      sql"""SELECT state_kind, state_payload, input_fingerprints, expires_at FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND step_id = $stepId AND step_scope_path = $stepScopePath AND step_version = $stepVersion""".query[
          (String, String, String, Option[java.time.Instant])
        ].option
    }.map { case (kind, payload, fingerprints, expiresAt) =>
      StoredStep(kind, payload, fingerprints, expiresAt)
    }

  /** Escapes `%`, `_`, and the escape character itself so the caller's prefix is
    * matched literally against the key with a trailing `%` wildcard.
    */
  private def likeEscaped(prefix: String): String =
    prefix.flatMap {
      case '\\' => "\\\\"
      case '%'  => "\\%"
      case '_'  => "\\_"
      case c    => c.toString
    }

  /** The columns backing [[WorkflowInstance.Info]], in `InfoSelect` order. */
  private type InfoRow = (
      String,
      Long,
      Long,
      Option[String],
      Option[String],
      Option[String],
      java.time.Instant,
      Option[java.time.Instant],
      Int,
      Option[String]
    )

  private val InfoSelect: Fragment =
    fr"""SELECT key, workflow_version_at_creation, generation,
         parent_workflow_id, parent_instance_key, parent_scope, created_at,
         last_run_at, times_executed, terminal_state
         FROM workflow_instances"""

  private def terminalEnum(state: String): WorkflowTerminalState = state match {
    case "completed"  => WorkflowTerminalState.Completed
    case "failed"     => WorkflowTerminalState.Failed
    case "cancelled"  => WorkflowTerminalState.Cancelled
    case "terminated" => WorkflowTerminalState.Terminated
    case other        => throw new StepSerializationFailed(s"Unknown terminal state '$other'")
  }

  private def toInfo(workflowId: WorkflowId, scope: String, r: InfoRow): WorkflowInstance.Info = {
    val (key, versionAtCreation, generation, parentWf, parentKey, parentScope, createdAt, lastRunAt, timesExecuted, terminal) = r
    val parentId = parentWf.map(pwf => WorkflowInstanceId(pwf, parentKey.getOrElse(""), parentScope.getOrElse("")))
    WorkflowInstance.Info(
      id = WorkflowInstanceId(workflowId, key, scope),
      parentId = parentId,
      generation = generation,
      terminalState = terminal.map(terminalEnum),
      workflowVersionAtCreation = versionAtCreation,
      createdAt = createdAt,
      lastRunAt = lastRunAt,
      timesExecuted = timesExecuted
    )
  }

  override def getWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: String = ""
  ): Vector[WorkflowInstance.Info] = {
    val pattern = likeEscaped(keyPrefix) + "%"
    runSync {
      (InfoSelect ++ fr"WHERE workflow_id = $workflowId AND scope = $scope AND key LIKE $pattern ESCAPE '\' ORDER BY key")
        .query[InfoRow]
        .to[Vector]
    }.map(toInfo(workflowId, scope, _))
  }

  override def getUnfinishedWorkflowInstances(
      workflowId: WorkflowId,
      includeWaiting: Boolean = false,
      limit: Int = -1
  ): Vector[WorkflowInstance.Info] = {
    val waitingCond = if (includeWaiting) Fragment.empty else fr"AND times_executed = 0"
    val limitFrag = if (limit > 0) fr"LIMIT $limit" else Fragment.empty
    runSync {
      (InfoSelect ++ fr"WHERE workflow_id = $workflowId AND scope = '' AND terminal_state IS NULL " ++
        waitingCond ++ fr"ORDER BY key " ++ limitFrag).query[InfoRow].to[Vector]
    }.map(toInfo(workflowId, "", _))
  }

  override def deleteWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: String = ""
  ): Long = {
    val pattern = likeEscaped(keyPrefix) + "%"
    runSync {
      for {
        _ <- sql"""DELETE FROM workflow_events
                   WHERE workflow_id = $workflowId AND scope = $scope AND key LIKE $pattern ESCAPE '\'""".update.run
        deleted <- sql"""DELETE FROM workflow_instances
                         WHERE workflow_id = $workflowId AND scope = $scope AND key LIKE $pattern ESCAPE '\'""".update.run
      } yield deleted.toLong
    }
  }

  /** Reads the terminal projection of an instance; `None` if the instance is
    * absent or not yet terminal.
    */
  private def readTerminal(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  ): Option[(String, Option[String])] =
    runSync {
      sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[
          (Option[String], Option[String])
        ].option
    }.flatMap { case (state, outcome) => state.map(s => (s, outcome)) }

  override def awaitResult[Out](
      instance: WorkflowInstance[?, Out],
      timeout: FiniteDuration
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    val workflow = instance.workflow
    val instanceId = instance.id
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val outCacheable = workflow.outputCacheable
    given Cacheable[Out] = outCacheable
    val completionCodec = summon[Cacheable[WorkflowCompletionResult[Out]]]

    val deadlineNanos = System.nanoTime() + timeout.toNanos
    var terminal: Option[(String, Option[String])] = None
    while (terminal.isEmpty && System.nanoTime() < deadlineNanos) {
      terminal = readTerminal(workflowId, key, scope)
      if (terminal.isEmpty && System.nanoTime() < deadlineNanos) Thread.sleep(AwaitResultPollIntervalMillis)
    }

    terminal match {
      case Some((state, outcome)) => handleTerminalRead(state, outcome, outCacheable, completionCodec, instanceId)
      case None =>
        throw new java.util.concurrent.TimeoutException(
          s"Workflow instance did not reach a terminal state within $timeout: $instanceId"
        )
    }
  }

  private[atomicflow] override def getWorkflowInstanceInfo[In, Out](
      instance: WorkflowInstance[In, Out]
  ): WorkflowInstance.Info = {
    val instanceId = instance.id
    val row = runSync {
      (InfoSelect ++ fr"WHERE workflow_id = ${instanceId.workflowId} AND key = ${instanceId.workflowInstanceKey} AND scope = ${instanceId.scope}")
        .query[InfoRow]
        .option
    }
    row match {
      case Some(r) => toInfo(instanceId.workflowId, instanceId.scope, r)
      case None    => throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
    }
  }

  override def getChildWorkflowInstances(parentId: WorkflowInstanceId): Vector[WorkflowInstance.Info] = {
    val rows = runSync {
      sql"""SELECT workflow_id, scope, key, workflow_version_at_creation, generation,
                   parent_workflow_id, parent_instance_key, parent_scope, created_at, last_run_at, times_executed, terminal_state
            FROM workflow_instances
            WHERE parent_workflow_id = ${parentId.workflowId}
              AND parent_instance_key = ${parentId.workflowInstanceKey}
              AND parent_scope = ${parentId.scope}
            ORDER BY workflow_id, key, scope""".query[(WorkflowId, String, InfoRow)].to[Vector]
    }
    rows.map { case (wf, scope, r) => toInfo(wf, scope, r) }
  }

  private final class PostgresExecution(
      val workerId: String,
      val fencingToken: Long,
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      instanceScope: String,
      val generation: Long,
      runLeaseDuration: FiniteDuration
  ) extends WorkflowExecution {

    private val instanceId = WorkflowInstanceId(workflowId, key, instanceScope)

    private val uncancellableDepth = new ThreadLocal[Int] {
      override def initialValue(): Int = 0
    }

    private val scopeStack = new ThreadLocal[Vector[String]] {
      override def initialValue(): Vector[String] = Vector.empty
    }

    private def inUncancellableRegion: Boolean = uncancellableDepth.get() > 0

    private[atomicflow] override def enterUncancellable(): Unit =
      uncancellableDepth.set(uncancellableDepth.get() + 1)

    private[atomicflow] override def exitUncancellable(): Unit =
      uncancellableDepth.set(uncancellableDepth.get() - 1)

    override def currentScope: String = scopeStack.get().mkString("/")

    private[atomicflow] override def pushScope(escapedSegment: String): Unit =
      scopeStack.set(scopeStack.get() :+ escapedSegment)

    private[atomicflow] override def popScope(): Unit =
      scopeStack.set(scopeStack.get().dropRight(1))

    override def now: java.time.Instant = theClock.instant()

    override def durableRetryThreshold: FiniteDuration = PostgresWorkflowRuntime.this.durableRetryThreshold

    override def renewLease(): Unit = {
      val now = theClock.instant()
      val expires = now.plus(java.time.Duration.ofNanos(runLeaseDuration.toNanos))
      val updated = runSync {
        sql"""UPDATE workflow_instances
              SET lease_expires_at = $expires
              WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                AND lease_owner = $workerId AND fencing_token = $fencingToken
                AND terminal_state IS NULL""".update.run
      }
      if (updated != 1) throw LeaseLostException(instanceId)
    }

    override def checkCancellation(): Unit = {
      if (!inUncancellableRegion) {
        val requestedAt = runSync {
          sql"""SELECT cancel_requested_at FROM workflow_instances
                WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope""".query[
              Option[java.time.Instant]
            ].unique
        }
        if (requestedAt.isDefined) throw WorkflowCancelledException()
      }
    }

    override def lookupStep(stepId: StepId, stepVersion: Long): Option[StoredStep] =
      readStepRow(workflowId, key, instanceScope, stepId.key, stepId.scope, stepVersion)

    override def writeStepStarted(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String
    ): Unit =
      fenced {
        writeStepStartedIO(stepId, stepVersion, stepKind, inputFingerprints, "", None)
      }

    private def writeStepStartedIO(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): ConnectionIO[Unit] = {
      val now = theClock.instant()
      sql"""INSERT INTO workflow_steps (workflow_id, key, scope, step_id, step_scope_path, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
            VALUES ($workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $stepKind, 'started', $payload, $inputFingerprints, $expiresAt, $now, $now)
            ON CONFLICT (workflow_id, key, scope, step_id, step_version, step_scope_path) DO UPDATE
            SET state_kind = 'started', state_payload = EXCLUDED.state_payload, input_fingerprints = EXCLUDED.input_fingerprints, expires_at = EXCLUDED.expires_at, updated_at = $now""".update.run.map(
        _ => ()
      )
    }

    override def writeStepSucceeded(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        writeStepSucceededIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
      }

    private def writeStepSucceededIO(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): ConnectionIO[Unit] = {
      val now = theClock.instant()
      sql"""INSERT INTO workflow_steps (workflow_id, key, scope, step_id, step_scope_path, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
            VALUES ($workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $stepKind, 'succeeded', $payload, $inputFingerprints, $expiresAt, $now, $now)
            ON CONFLICT (workflow_id, key, scope, step_id, step_version, step_scope_path) DO UPDATE
            SET state_kind = 'succeeded', state_payload = EXCLUDED.state_payload, input_fingerprints = EXCLUDED.input_fingerprints, expires_at = EXCLUDED.expires_at, updated_at = $now""".update.run.map(
        _ => ()
      )
    }

    override def writeStepFailed(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        writeStepFailedIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
      }

    private def writeStepFailedIO(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): ConnectionIO[Unit] = {
      val now = theClock.instant()
      sql"""INSERT INTO workflow_steps (workflow_id, key, scope, step_id, step_scope_path, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
            VALUES ($workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $stepKind, 'failed', $payload, $inputFingerprints, $expiresAt, $now, $now)
            ON CONFLICT (workflow_id, key, scope, step_id, step_version, step_scope_path) DO UPDATE
            SET state_kind = 'failed', state_payload = EXCLUDED.state_payload, input_fingerprints = EXCLUDED.input_fingerprints, expires_at = EXCLUDED.expires_at, updated_at = $now""".update.run.map(
        _ => ()
      )
    }

    override def deleteStep(stepId: StepId, stepVersion: Long): Unit =
      fenced {
        sql"""DELETE FROM workflow_steps
              WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope} AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run
      }

    private val RetryLeafIdx: Int = Int.MaxValue

    private def insertRetrySubscriptionIO(stepId: StepId, stepVersion: Long, deadline: java.time.Instant): ConnectionIO[Unit] =
      for {
        _ <- sql"""DELETE FROM workflow_timer_subscriptions
                   WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                     AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion AND leaf_idx = $RetryLeafIdx""".update.run
        _ <- sql"""INSERT INTO workflow_timer_subscriptions
                    (subscription_id, workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, deadline)
                  VALUES (gen_random_uuid(), $workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $RetryLeafIdx, $deadline)
                  ON CONFLICT (workflow_id, key, scope, step_id, step_version, leaf_idx, step_scope_path) DO NOTHING""".update.run
      } yield ()

    override def suspendStepRetry(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        retryPayload: String,
        deadline: java.time.Instant,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        for {
          _ <- writeStepStartedIO(stepId, stepVersion, stepKind, inputFingerprints, retryPayload, expiresAt)
          _ <- insertRetrySubscriptionIO(stepId, stepVersion, deadline)
        } yield ()
      }

    override def fireDueStepRetries(stepId: StepId, stepVersion: Long): Unit =
      fenced {
        val now = theClock.instant()
        for {
          due <- sql"""SELECT subscription_id FROM workflow_timer_subscriptions
                       WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                         AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion AND leaf_idx = $RetryLeafIdx
                         AND deadline <= $now
                       ORDER BY deadline, subscription_id
                       FOR UPDATE""".query[java.util.UUID].to[Vector]
          _ <- due.traverse_ { subId =>
            for {
              exists <- sql"""SELECT 1 FROM workflow_events
                              WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                                AND event_kind = 'TimerFired' AND event_key = ${subId.toString}""".query[Int].option
              _ <- if (exists.isEmpty) appendEvent(workflowId, key, instanceScope, "TimerFired", subId.toString, "")
                   else ().pure[ConnectionIO]
            } yield ()
          }
        } yield ()
      }

    override def readStepRetryCandidates(stepId: StepId, stepVersion: Long): Vector[AwaitTimerCandidate] =
      runSync {
        for {
          subIds <- sql"""SELECT subscription_id FROM workflow_timer_subscriptions
                          WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                            AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion AND leaf_idx = $RetryLeafIdx""".query[
              java.util.UUID
            ].to[List]
          events <- if (subIds.isEmpty) Vector.empty[AwaitTimerCandidate].pure[ConnectionIO]
                    else {
                      val idStrings = subIds.map(_.toString)
                      (fr"""SELECT sequence_id, event_key, created_at FROM workflow_events
                            WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                              AND event_kind = 'TimerFired' AND event_key = ANY($idStrings)
                            ORDER BY sequence_id""").query[(Long, String, java.time.Instant)].to[Vector]
                        .map(_.map { case (seq, ek, createdAt) =>
                          AwaitTimerCandidate(seq, java.util.UUID.fromString(ek), createdAt)
                        })
                    }
        } yield events
      }

    override def resolveStepRetry(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        stateKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        for {
          _ <- if (stateKind == "succeeded")
            writeStepSucceededIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
          else
            writeStepFailedIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
          _ <- deleteTimerSubscriptionsIO(stepId, stepVersion)
        } yield ()
      }

    override def deleteStepRetry(stepId: StepId, stepVersion: Long): Unit =
      fenced {
        for {
          _ <- sql"""DELETE FROM workflow_steps
                     WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope} AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run
          _ <- deleteTimerSubscriptionsIO(stepId, stepVersion)
        } yield ()
      }

    override def readAwaitSignalCandidates(signalKey: SignalKey): Vector[AwaitSignalCandidate] =
      runSync {
        for {
          cursor <- sql"""SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                          WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope AND signal_key = $signalKey""".query[Long].unique
          events <- sql"""SELECT sequence_id, payload, created_at FROM workflow_events
                          WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                            AND event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor
                          ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
        } yield events.map { case (seq, payload, createdAt) => AwaitSignalCandidate(seq, payload, createdAt) }
      }

    override def resolveAwaitSignal(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        signalKey: SignalKey,
        winningSequenceId: Long,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        for {
          _ <- writeStepSucceededIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
          _ <- advanceCursorIO(signalKey, winningSequenceId)
          _ <- deleteSubscriptionsIO(stepId, stepVersion)
        } yield ()
      }

    override def suspendAwaitSignal(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        signalKey: SignalKey,
        expiresAt: Option[java.time.Instant]
    )(decide: Vector[AwaitSignalCandidate] => Option[(Long, String)]): Option[String] =
      fencedVal {
        for {
          _ <- upsertSubscriptionIO(stepId, stepVersion, signalKey)
          candidates <- readCandidatesIO(signalKey)
          decision = decide(candidates)
          resolved <- decision match {
            case Some((winSeq, payload)) =>
              for {
                _ <- writeStepSucceededIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
                _ <- advanceCursorIO(signalKey, winSeq)
                _ <- deleteSubscriptionsIO(stepId, stepVersion)
              } yield Some(payload)
            case None => Option.empty[String].pure[ConnectionIO]
          }
        } yield resolved
      }

    override def fireDueTimers(stepId: StepId, stepVersion: Long): Unit =
      fireDueTimerLeaves(stepId, stepVersion)

    override def readAwaitTimerCandidates(stepId: StepId, stepVersion: Long): Vector[AwaitTimerCandidate] =
      runSync {
        for {
          subIds <- sql"""SELECT subscription_id FROM workflow_timer_subscriptions
                          WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                            AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion AND leaf_idx = 0""".query[
              java.util.UUID
            ].to[List]
          events <- if (subIds.isEmpty) Vector.empty[AwaitTimerCandidate].pure[ConnectionIO]
                    else {
                      val idStrings = subIds.map(_.toString)
                      (fr"""SELECT sequence_id, event_key, created_at FROM workflow_events
                            WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                              AND event_kind = 'TimerFired' AND event_key = ANY($idStrings)
                            ORDER BY sequence_id""").query[(Long, String, java.time.Instant)].to[Vector]
                        .map(_.map { case (seq, ek, createdAt) =>
                          AwaitTimerCandidate(seq, java.util.UUID.fromString(ek), createdAt)
                        })
                    }
        } yield events
      }

    override def resolveAwaitTimer(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        payload: String,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        for {
          _ <- writeStepSucceededIO(stepId, stepVersion, stepKind, inputFingerprints, payload, expiresAt)
          _ <- deleteTimerSubscriptionsIO(stepId, stepVersion)
        } yield ()
      }

    override def suspendAwaitTimer(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        deadline: java.time.Instant,
        expiresAt: Option[java.time.Instant]
    ): Unit =
      fenced {
        for {
          existing <- sql"""SELECT subscription_id FROM workflow_timer_subscriptions
                            WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                              AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion AND leaf_idx = 0""".query[
              java.util.UUID
            ].option
          _ <- existing match {
            case Some(_) => ().pure[ConnectionIO]
            case None =>
              sql"""INSERT INTO workflow_timer_subscriptions
                      (subscription_id, workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, deadline)
                    VALUES (gen_random_uuid(), $workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, 0, $deadline)
                    ON CONFLICT (workflow_id, key, scope, step_id, step_version, leaf_idx, step_scope_path) DO NOTHING""".update.run.map(_ => ())
          }
        } yield ()
      }

    override def invalidateTimer(stepId: StepId, stepVersion: Long, deadline: java.time.Instant): Unit =
      fenced {
        for {
          _ <- sql"""DELETE FROM workflow_steps
                     WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                       AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run
          _ <- deleteTimerSubscriptionsIO(stepId, stepVersion)
          _ <- sql"""INSERT INTO workflow_timer_subscriptions
                      (subscription_id, workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, deadline)
                    VALUES (gen_random_uuid(), $workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, 0, $deadline)
                    ON CONFLICT (workflow_id, key, scope, step_id, step_version, leaf_idx, step_scope_path) DO NOTHING""".update.run
        } yield ()
      }

    override def fireDueTimerLeaves(stepId: StepId, stepVersion: Long): Unit =
      fenced {
        val now = theClock.instant()
        for {
          due <- sql"""SELECT subscription_id FROM workflow_timer_subscriptions
                       WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                         AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion
                         AND deadline <= $now
                       ORDER BY deadline, subscription_id
                       FOR UPDATE""".query[java.util.UUID].to[Vector]
          _ <- due.traverse_ { subId =>
            for {
              exists <- sql"""SELECT 1 FROM workflow_events
                              WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                                AND event_kind = 'TimerFired' AND event_key = ${subId.toString}""".query[Int].option
              _ <- if (exists.isEmpty) appendEvent(workflowId, key, instanceScope, "TimerFired", subId.toString, "")
                   else ().pure[ConnectionIO]
            } yield ()
          }
        } yield ()
      }

    override def evaluateAwaitRace(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String,
        leaves: Vector[AwaitRaceLeaf],
        expiresAt: Option[java.time.Instant]
    )(decide: Vector[AwaitRaceCandidate] => Option[AwaitRaceDecision]): Option[String] =
      fencedVal {
        for {
          _ <- registerAllSubscriptionsIO(stepId, stepVersion, leaves)
          candidates <- readAllCandidatesIO(stepId, stepVersion, leaves)
          decision = decide(candidates)
          resolved <- decision match {
            case Some(d) =>
              for {
                _ <- writeStepSucceededIO(stepId, stepVersion, stepKind, inputFingerprints, d.payload, expiresAt)
                _ <- d.advanceSignalKey.traverse_(k => advanceCursorIO(k, d.winningSequenceId))
                _ <- deleteAllSubscriptionsIO(stepId, stepVersion)
              } yield Some(d.payload)
            case None => Option.empty[String].pure[ConnectionIO]
          }
        } yield resolved
      }

    private def registerAllSubscriptionsIO(
        stepId: StepId,
        stepVersion: Long,
        leaves: Vector[AwaitRaceLeaf]
    ): ConnectionIO[Unit] =
      leaves.traverse_ {
        case l: AwaitRaceSignalLeaf =>
          sql"""INSERT INTO workflow_signal_subscriptions (workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, signal_key)
                VALUES ($workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, ${l.leafIdx}, ${l.signalKey})
                ON CONFLICT (workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, signal_key) DO NOTHING""".update.run.map(_ => ())
        case l: AwaitRaceTimerLeaf =>
          sql"""INSERT INTO workflow_timer_subscriptions
                  (subscription_id, workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, deadline)
                VALUES (gen_random_uuid(), $workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, ${l.leafIdx}, ${l.deadline})
                ON CONFLICT (workflow_id, key, scope, step_id, step_version, leaf_idx, step_scope_path) DO NOTHING""".update.run.map(_ => ())
        case l: AwaitRaceCompletionLeaf =>
          sql"""INSERT INTO workflow_completion_subscriptions
                  (workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, completed_workflow_id, completed_key, completed_scope)
                VALUES ($workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, ${l.leafIdx}, ${l.completedWorkflowId}, ${l.completedKey}, ${l.completedScope})
                ON CONFLICT (workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, completed_workflow_id, completed_key, completed_scope) DO NOTHING""".update.run.map(_ => ())
      }

    private def readAllCandidatesIO(
        stepId: StepId,
        stepVersion: Long,
        leaves: Vector[AwaitRaceLeaf]
    ): ConnectionIO[Vector[AwaitRaceCandidate]] =
      for {
        signal <- leaves.collect { case l: AwaitRaceSignalLeaf => l }.traverse(readSignalLeafIO).map(_.flatten)
        timer <- leaves
          .collect { case l: AwaitRaceTimerLeaf => l }
          .traverse(l => readTimerLeafIO(stepId, stepVersion, l))
          .map(_.flatten)
        completion <- leaves.collect { case l: AwaitRaceCompletionLeaf => l }.traverse(readCompletionLeafIO).map(_.flatten)
      } yield signal ++ timer ++ completion

    private def readSignalLeafIO(l: AwaitRaceSignalLeaf): ConnectionIO[Vector[AwaitRaceCandidate]] =
      for {
        cursor <- sql"""SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                        WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope AND signal_key = ${l.signalKey}""".query[Long].unique
        events <- sql"""SELECT sequence_id, payload, created_at FROM workflow_events
                        WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                          AND event_kind = 'Signal' AND event_key = ${l.signalKey} AND sequence_id > $cursor
                        ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
      } yield events.map { case (seq, payload, createdAt) =>
        AwaitRaceCandidate(seq, l.leafIdx, payload, createdAt)
      }

    private def readTimerLeafIO(
        stepId: StepId,
        stepVersion: Long,
        l: AwaitRaceTimerLeaf
    ): ConnectionIO[Vector[AwaitRaceCandidate]] =
      for {
        subId <- sql"""SELECT subscription_id FROM workflow_timer_subscriptions
                       WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                         AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion AND leaf_idx = ${l.leafIdx}""".query[java.util.UUID].option
        events <- subId match {
          case None => Vector.empty[AwaitRaceCandidate].pure[ConnectionIO]
          case Some(id) =>
            sql"""SELECT sequence_id, created_at FROM workflow_events
                  WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                    AND event_kind = 'TimerFired' AND event_key = ${id.toString}
                  ORDER BY sequence_id""".query[(Long, java.time.Instant)].to[Vector]
              .map(_.map { case (seq, createdAt) => AwaitRaceCandidate(seq, l.leafIdx, "", createdAt) })
        }
      } yield events

    private def readCompletionLeafIO(l: AwaitRaceCompletionLeaf): ConnectionIO[Vector[AwaitRaceCandidate]] =
      for {
        events <- sql"""SELECT sequence_id, payload, created_at FROM workflow_events
                        WHERE workflow_id = ${l.completedWorkflowId} AND key = ${l.completedKey} AND scope = ${l.completedScope}
                          AND event_kind = 'WorkflowCompleted' AND event_key = ''
                        ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
      } yield events.map { case (seq, payload, createdAt) => AwaitRaceCandidate(seq, l.leafIdx, payload, createdAt) }

    private def deleteAllSubscriptionsIO(stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
      for {
        _ <- sql"""DELETE FROM workflow_signal_subscriptions
                   WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                     AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run
        _ <- sql"""DELETE FROM workflow_timer_subscriptions
                   WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                     AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run
        _ <- sql"""DELETE FROM workflow_completion_subscriptions
                   WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
                     AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run
      } yield ()

    private def deleteTimerSubscriptionsIO(stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
      sql"""DELETE FROM workflow_timer_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope}
              AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run.map(_ => ())

    private def advanceCursorIO(signalKey: SignalKey, sequenceId: Long): ConnectionIO[Unit] =
      sql"""INSERT INTO signal_cursor (workflow_id, key, scope, signal_key, sequence_id)
            VALUES ($workflowId, $key, $instanceScope, $signalKey, $sequenceId)
            ON CONFLICT (workflow_id, key, scope, signal_key) DO UPDATE
            SET sequence_id = EXCLUDED.sequence_id""".update.run.map(_ => ())

    private def deleteSubscriptionsIO(stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
      sql"""DELETE FROM workflow_signal_subscriptions
            WHERE workflow_id = $workflowId AND key = $key AND scope = ${instanceScope} AND step_id = ${stepId.key} AND step_scope_path = ${stepId.scope} AND step_version = $stepVersion""".update.run.map(
        _ => ()
      )

    private def upsertSubscriptionIO(stepId: StepId, stepVersion: Long, signalKey: SignalKey): ConnectionIO[Unit] =
      sql"""INSERT INTO workflow_signal_subscriptions (workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, signal_key)
            VALUES ($workflowId, $key, ${instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, 0, $signalKey)
            ON CONFLICT (workflow_id, key, scope, step_id, step_scope_path, step_version, leaf_idx, signal_key) DO NOTHING""".update.run.map(
        _ => ()
      )

    private def readCandidatesIO(signalKey: SignalKey): ConnectionIO[Vector[AwaitSignalCandidate]] =
      for {
        cursor <- sql"""SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                        WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope AND signal_key = $signalKey""".query[Long].unique
        events <- sql"""SELECT sequence_id, payload, created_at FROM workflow_events
                        WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                          AND event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor
                        ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
      } yield events.map { case (seq, payload, createdAt) => AwaitSignalCandidate(seq, payload, createdAt) }

    /** Runs `write` inside one transaction, guarded by an exclusive lock on the
      * instance row and a fencing check; throws [[LeaseLostException]] if the
      * lease no longer belongs to this run, affecting no rows.
      */
    private def fenced[A](write: ConnectionIO[A]): Unit = {
      fencedVal(write)
      ()
    }

    /** Like [[fenced]] but returns the value `write` produced, or throws
      * [[LeaseLostException]] if the fence failed.
      */
    private def fencedVal[A](write: ConnectionIO[A]): A = {
      val res: Option[A] = runSync {
        for {
          _ <- sql"""SELECT 1 FROM workflow_instances
                     WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                     FOR UPDATE""".query[Int].unique
          fenceOk <- sql"""SELECT 1 FROM workflow_instances
                           WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                             AND lease_owner = $workerId AND fencing_token = $fencingToken""".query[Int].option
          r <- if (fenceOk.isDefined) write.map(Some(_)) else None.pure[ConnectionIO]
        } yield r
      }
      res match {
        case Some(a) => a
        case None    => throw LeaseLostException(instanceId)
      }
    }
  }

  private[atomicflow] override def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit = {
    val now = theClock.instant()
    val scheduledAt = now.plus(java.time.Duration.ofNanos(delay.toNanos))
    runSync {
      sql"""
        INSERT INTO workflow_wakeups (workflow_id, key, scope, created_at, scheduled_at, attempts)
        VALUES (${instanceId.workflowId}, ${instanceId.workflowInstanceKey}, ${instanceId.scope}, $now, $scheduledAt, 0)
        ON CONFLICT (workflow_id, key, scope) DO NOTHING
      """.update.run
    }
    ()
  }

  /** The instance's terminal state, or `None` if not yet terminal. Used by the
    * job runner to classify a run's outcome by durable state rather than by
    * exception type.
    */
  private[atomicflow] def readTerminalState(instanceId: WorkflowInstanceId): Option[String] =
    runSync {
      sql"""SELECT terminal_state FROM workflow_instances
            WHERE workflow_id = ${instanceId.workflowId} AND key = ${instanceId.workflowInstanceKey} AND scope = ${instanceId.scope}""".query[
          Option[String]
        ].unique
    }

  /** Whether the instance's lease is still held by `worker` at `token`. */
  private[atomicflow] def leaseStillOurs(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Boolean =
    runSync {
      sql"""SELECT 1 FROM workflow_instances
            WHERE workflow_id = ${instanceId.workflowId} AND key = ${instanceId.workflowInstanceKey} AND scope = ${instanceId.scope}
              AND lease_owner = $worker AND fencing_token = $token""".query[Int].option
    }.isDefined

  private val runnerGuard = new Object
  @volatile private var activeRunner: PostgresJobRunner = null

  private[atomicflow] def isRunnerActive: Boolean = {
    val r = activeRunner
    r != null && r.isActive
  }

  private[atomicflow] def clearActiveRunner(runner: PostgresJobRunner): Unit =
    runnerGuard.synchronized {
      if (activeRunner eq runner) activeRunner = null
    }

  override def startJobRunner(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings = JobRunnerSettings.default
  ): JobRunner =
    createRunner(definitions, settings, startLoop = true)

  /** Package-private test hook: like [[startJobRunner]] but builds a runner whose
    * background driver loop is NOT started, so a test can drive claim cycles
    * deterministically via `runDriverCycle` without racing the auto-loop.
    */
  private[atomicflow] def startJobRunnerForTests(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings
  ): PostgresJobRunner =
    createRunner(definitions, settings, startLoop = false)

  private def createRunner(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings,
      startLoop: Boolean
  ): PostgresJobRunner =
    runnerGuard.synchronized {
      if (isRunnerActive)
        throw new IllegalStateException(
          "This runtime already has an active job runner; stop it before starting another"
        )
      val runner = new PostgresJobRunner(this, definitions, settings, startLoop)
      activeRunner = runner
      runner
    }
}
