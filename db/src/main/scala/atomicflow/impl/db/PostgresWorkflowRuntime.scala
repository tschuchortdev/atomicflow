package atomicflow.impl.db

import atomicflow.*
import atomicflow.internal.{Framing, ScopePath}
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

import java.time.Clock
import java.time.Instant
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
  * monotonic `fencing_token`. Worker identity is an opaque string recorded in
  * `lease_owner`: caller-thread runs use `"processUuid:threadId"` (with
  * `processUuid` generated once per runtime instance), and each job runner names
  * itself through its `JobRunnerSettings`. Nothing interprets the strings —
  * leases compare `lease_owner`/`fencing_token` pairs — so any stable unique
  * value is acceptable.
  */
class PostgresWorkflowRuntime private[atomicflow] (
    ds: DataSource,
    theClock: Clock,
    leaseDuration: FiniteDuration = 5.minutes,
    leaseAcquireTimeout: FiniteDuration = 30.seconds,
    durableRetryThresholdSetting: FiniteDuration = 30.seconds
)(using ec: ExecutionContext)
    extends WorkflowRuntime {

  Flyway.configure().dataSource(ds).load().migrate()

  private val log = LoggerFactory.getLogger(getClass)

  private val xa = Transactor.fromDataSource[IO](ds, ec)

  /** Doobie array support for the structured scope arrays: scopes are stored as
    * raw `TEXT[]` segment arrays (see `spec/sub-workflows-iteration.md`, "Key
    * derivation") and mapped to `Vector[String]` at the Scala boundary. The
    * element/array types are pinned to `text`/`_text` so the bound parameter is
    * `TEXT[]` and compares equal to the column (a plain `Array[String]` binds
    * as `varchar[]`, which Postgres refuses to compare against `TEXT[]`).
    */
  private val scopeTextArrayMeta: Meta[Array[String]] = Meta.Advanced.array[String]("text", "_text")
  implicit val scopeVectorPut: Put[Vector[String]] = scopeTextArrayMeta.put.contramap(_.toArray)
  implicit val scopeVectorGet: Get[Vector[String]] = scopeTextArrayMeta.get.map(_.toVector)

  private val processUuid: String = java.util.UUID.randomUUID().toString

  /** Well-known advisory-lock key for the global event-append protocol. */
  private val EventAppendLockKey: Long = 844810493715605001L

  /** Poll cadence of the passive `awaitResult` waiter. */
  private val AwaitResultPollIntervalMillis: Long = 50L

  private def clearInterrupt(): Unit = Thread.interrupted()

  private[atomicflow] def runTransaction[A](fa: ConnectionIO[A]): A = {
    clearInterrupt()
    fa.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)
  }

  /** Whether `t`'s cause chain contains a `java.sql.SQLException` whose
    * SQLSTATE is 40P01 (deadlock). Deadlocks are transient: both wakeup passes
    * are idempotent, so the caller may simply retry the whole pass.
    */
  private def isDeadlock(t: Throwable): Boolean = {
    @annotation.tailrec
    def loop(c: Throwable): Boolean =
      c match {
        case null                                   => false
        case e: java.sql.SQLException if e.getSQLState == "40P01" => true
        case other                                  => loop(other.getCause)
      }
    loop(t)
  }

  /** Bounded retry around a deadlockable pass. The two full-scan passes
    * (`dispatchNewEvents`, `scanAllSubscriptions`) upsert wakeups for many
    * instances in one transaction while a concurrent terminal transition /
    * child-close upserts wakeups for other instances; the opposite lock orders
    * over `workflow_instances` (FK KEY SHARE) and `workflow_wakeups` can raise
    * SQLSTATE 40P01. Both passes are idempotent, so a bounded retry with a short
    * backoff lets one side make progress and the retried pass succeed.
    */
  private def withDeadlockRetry[A](fa: => A, attempts: Int = 5, backoffMillis: Long = 50L): A = {
    var attempt = 0
    var last: Throwable = null
    while (attempt < attempts) {
      try {
        return fa
      } catch {
        case t: Throwable if isDeadlock(t) =>
          last = t
          attempt += 1
          if (attempt < attempts) Thread.sleep(backoffMillis)
      }
    }
    throw last
  }

  private def workerId: String = s"$processUuid:${Thread.currentThread().getId}"

  private[atomicflow] def leaseExpiry(now: java.time.Instant, duration: FiniteDuration): java.time.Instant =
    now.plus(java.time.Duration.ofNanos(duration.toNanos))

  override def clock: Clock = theClock

  /** This runtime's per-run handle: the fencing identity of one run (worker,
    * fencing token, generation) plus the instance identity, created when the
    * run's lease is acquired. The alias targets the class projection so the
    * member type does not depend on a particular runtime instance.
    */
  override type CurrentExecution = PostgresWorkflowRuntime#PostgresCurrentExecution

  def durableRetryThreshold: FiniteDuration = durableRetryThresholdSetting

  override def createWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceKey: WorkflowInstanceKey,
      in: In
  )(using cacheable: Cacheable[In]): WorkflowInstance[In, Out] = {
    val workflowId = workflow.id
    val instanceId = WorkflowInstanceId(workflowId, instanceKey)
    val serializedInput = cacheable.write(in)

    val (inserted, existing) = runTransaction {
      for {
        inserted <- sql"""
          INSERT INTO workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope, input, workflow_version_at_creation)
          VALUES ($workflowId, $instanceKey, '{}', $serializedInput, ${workflow.version})
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
        """.update.run
        existing <- if (inserted == 0)
          sql"SELECT input FROM workflow_instances WHERE workflow_id = $workflowId AND workflow_instance_key = $instanceKey AND workflow_instance_scope = '{}'"
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
    val deleted = runTransaction {
      for {
        deleted <- sql"""
          DELETE FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_key = $instanceKey AND workflow_instance_scope = '{}'
        """.update.run
        _ <- sql"""
          INSERT INTO workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope, input, workflow_version_at_creation)
          VALUES ($workflowId, $instanceKey, '{}', $serializedInput, ${workflow.version})
        """.update.run
      } yield deleted
    }
    deleted > 0
  }

  /** Reads the boundary timestamp of `stepId` on the instance: the last-updated
    * timestamp of the step row with the highest version (used to decide which
    * cached history is "before" vs. "after" the selected step). `None` means the
    * step has no cached row (unknown or never executed).
    */
  private def readRestartBoundaryIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      stepId: StepId
  ): ConnectionIO[Option[java.time.Instant]] =
    sql"""SELECT updated_at FROM workflow_steps
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
            AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope}
          ORDER BY step_version DESC
          LIMIT 1""".query[java.time.Instant].option

  override def forkWorkflow[In, Out](
      sourceInstanceId: WorkflowInstanceId,
      newInstanceKey: WorkflowInstanceKey,
      restartFromStep: StepId
  )(using workflow: Workflow[In, Out]): WorkflowInstance[In, Out] = {
    val sourceWf = sourceInstanceId.workflowId
    val sourceKey = sourceInstanceId.workflowInstanceKey
    val sourceScope = sourceInstanceId.scope
    val newWorkflowId = workflow.id
    val newId = WorkflowInstanceId(newWorkflowId, newInstanceKey, Vector.empty)

    runTransaction {
      for {
        sourceInput <- sql"""SELECT input FROM workflow_instances
                             WHERE workflow_id = $sourceWf AND workflow_instance_key = $sourceKey AND workflow_instance_scope = $sourceScope
                             FOR UPDATE""".query[
            String
          ].option
        _ <- sourceInput match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $sourceInstanceId")
          case _ => ().pure[ConnectionIO]
        }
        boundary <- readRestartBoundaryIO(sourceWf, sourceKey, sourceScope, restartFromStep)
        _ <- boundary match {
          case None =>
            throw new InvalidRestartStepException(
              s"restartFromStep ${restartFromStep.key}@'${restartFromStep.scope.mkString("/")}' does not identify an executed step of $sourceInstanceId"
            )
          case _ => ().pure[ConnectionIO]
        }
        inserted <- sql"""INSERT INTO workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope, input, workflow_version_at_creation)
                          VALUES ($newWorkflowId, $newInstanceKey, '{}', $sourceInput, ${workflow.version})
                          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run
        _ <- if (inserted == 0)
          throw new WorkflowInputConflictException(
            s"Workflow instance already exists under the fork key: $newId"
          )
        else ().pure[ConnectionIO]
        _ <- sql"""INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
                   SELECT $newWorkflowId, $newInstanceKey, '{}', step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at
                   FROM workflow_steps
                   WHERE workflow_id = $sourceWf AND workflow_instance_key = $sourceKey AND workflow_instance_scope = $sourceScope
                     AND step_kind <> 'ChildClose'
                     AND updated_at < $boundary""".update.run
        _ <- upsertWakeupIO(newId.workflowId, newInstanceKey, Vector.empty, theClock.instant())
      } yield ()
    }
    WorkflowInstance(workflow, newId)
  }

  override def resetWorkflow[In, Out](
      sourceInstanceId: WorkflowInstanceId,
      restartFromStep: StepId
  )(using workflow: Workflow[In, Out]): Unit = {
    val wf = sourceInstanceId.workflowId
    val key = sourceInstanceId.workflowInstanceKey
    val scope = sourceInstanceId.scope
    val now = theClock.instant()
    runTransaction {
      for {
        terminal <- sql"""SELECT terminal_state FROM workflow_instances
                          WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                          FOR UPDATE""".query[Option[String]].option
        _ <- terminal match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $sourceInstanceId")
          case Some(Some(_)) =>
            throw new IllegalStateException(s"Cannot reset a terminal workflow instance: $sourceInstanceId")
          case Some(None) => ().pure[ConnectionIO]
        }
        boundary <- readRestartBoundaryIO(wf, key, scope, restartFromStep)
        _ <- boundary match {
          case None =>
            throw new InvalidRestartStepException(
              s"restartFromStep ${restartFromStep.key}@'${restartFromStep.scope.mkString("/")}' does not identify an executed step of $sourceInstanceId"
            )
          case _ => ().pure[ConnectionIO]
        }
        _ <- sql"""DELETE FROM workflow_signal_subscriptions s
                   USING workflow_steps st
                   WHERE s.workflow_id = $wf AND s.workflow_instance_key = $key AND s.workflow_instance_scope = $scope
                     AND st.workflow_id = $wf AND st.workflow_instance_key = $key AND st.workflow_instance_scope = $scope
                     AND st.updated_at >= $boundary
                     AND st.step_id = s.step_id AND st.step_scope_within_instance = s.step_scope_within_instance AND st.step_version = s.step_version""".update.run
        _ <- sql"""DELETE FROM workflow_timer_subscriptions s
                   USING workflow_steps st
                   WHERE s.workflow_id = $wf AND s.workflow_instance_key = $key AND s.workflow_instance_scope = $scope
                     AND st.workflow_id = $wf AND st.workflow_instance_key = $key AND st.workflow_instance_scope = $scope
                     AND st.updated_at >= $boundary
                     AND st.step_id = s.step_id AND st.step_scope_within_instance = s.step_scope_within_instance AND st.step_version = s.step_version""".update.run
        _ <- sql"""DELETE FROM workflow_completion_subscriptions s
                   USING workflow_steps st
                   WHERE s.workflow_id = $wf AND s.workflow_instance_key = $key AND s.workflow_instance_scope = $scope
                     AND st.workflow_id = $wf AND st.workflow_instance_key = $key AND st.workflow_instance_scope = $scope
                     AND st.updated_at >= $boundary
                     AND st.step_id = s.step_id AND st.step_scope_within_instance = s.step_scope_within_instance AND st.step_version = s.step_version""".update.run
        _ <- sql"""DELETE FROM workflow_steps
                   WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                     AND step_kind <> 'ChildClose'
                     AND updated_at >= $boundary""".update.run
        _ <- sql"""DELETE FROM workflow_wakeups
                   WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
        _ <- sql"""UPDATE workflow_instances SET generation = generation + 1
                   WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
        _ <- upsertWakeupIO(wf, key, scope, now)
      } yield ()
    }
    ()
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

  /** Decodes the `inherit_signals` column back into a [[SignalInheritance]].
    * `none`/`all` are plain tokens; `some:` carries a JSON array of prefixes
    * (the same unambiguous encoding produced by [[encodeSignalInheritance]]).
    */
  private def decodeSignalInheritance(s: Option[String]): SignalInheritance = s match {
    case None | Some("none") => SignalInheritance.none
    case Some("all")         => SignalInheritance.all
    case Some(x) if x.startsWith("some:") =>
      SignalInheritance.some(upickle.default.read[Seq[String]](x.drop("some:".length)))
    case other => throw new StepSerializationFailed(s"Unknown inherit_signals value '$other'")
  }

  /** Whether a stored inheritance selector permits an awaited key. `all` permits
    * every key; `some(prefixes)` permits keys with a matching prefix; `none`
    * permits nothing.
    */
  private def inheritancePermits(si: SignalInheritance, key: SignalKey): Boolean = si match {
    case SignalInheritance.none           => false
    case SignalInheritance.all            => true
    case SignalInheritance.some(prefixes) => prefixes.exists(key.startsWith)
  }

  /** Replays a child's inheritance configuration on a `startAsChild` re-run
    * (replaying the parent replaces the current policy per the spec). When the
    * stored configuration differs from the replayed one it is overwritten and a
    * wakeup is durably scheduled for the affected descendant subtree, so a
    * suspended descendant whose await now matches retained events is re-run and
    * the run-time eligibility check exposes them. This is intentionally
    * straightforward (a full descendant-subtree walk) rather than optimized; see
    * the `child-signal-inheritance.md` caveat that policy updates may be
    * expensive. Narrowing also schedules wakeups, which is harmless because the
    * candidate query re-applies the narrowed policy.
    */
  private def updateChildInheritanceIO(
      cwf: WorkflowId,
      ckey: WorkflowInstanceKey,
      cscope: Vector[String],
      newPolicy: String,
      newPast: Boolean
  ): ConnectionIO[Unit] =
    for {
      cur <- sql"""SELECT inherit_signals, inherit_past_events FROM workflow_instances
                   WHERE workflow_id = $cwf AND workflow_instance_key = $ckey AND workflow_instance_scope = $cscope""".query[(Option[String], Boolean)].option
      _ <- cur match {
        case Some((Some(p), past)) if p == newPolicy && past == newPast => ().pure[ConnectionIO]
        case _ =>
          for {
            _ <- sql"""UPDATE workflow_instances SET inherit_signals = $newPolicy, inherit_past_events = $newPast
                       WHERE workflow_id = $cwf AND workflow_instance_key = $ckey AND workflow_instance_scope = $cscope""".update.run
            _ <- scheduleDescendantWakeupsIO(cwf, ckey, cscope)
          } yield ()
      }
    } yield ()

  /** Coalesced durable wakeups for the descendant subtree rooted at
    * `(wf, k, s)`, including the root itself, for every node that has a pending
    * signal subscription. Used on policy updates so suspended descendants
    * re-evaluate their awaits under the new configuration.
    */
  private def scheduleDescendantWakeupsIO(
      wf: WorkflowId,
      k: WorkflowInstanceKey,
      s: Vector[String]
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    def childrenOf(
        w: WorkflowId,
        kk: WorkflowInstanceKey,
        ss: Vector[String]
    ): ConnectionIO[Vector[(WorkflowId, WorkflowInstanceKey, Vector[String])]] =
      sql"""SELECT workflow_id, workflow_instance_key, workflow_instance_scope FROM workflow_instances
            WHERE parent_workflow_id = $w AND parent_instance_key = $kk AND parent_instance_scope = $ss""".query[
          (WorkflowId, WorkflowInstanceKey, Vector[String])
        ].to[Vector]
    def wake(w: WorkflowId, kk: WorkflowInstanceKey, ss: Vector[String]): ConnectionIO[Unit] =
      for {
        has <- sql"""SELECT 1 FROM workflow_signal_subscriptions
                     WHERE workflow_id = $w AND workflow_instance_key = $kk AND workflow_instance_scope = $ss LIMIT 1""".query[Int].option
        _ <- if (has.isDefined)
          sql"""INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
                VALUES ($w, $kk, $ss, $now, $now, 0)
                ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run.map(_ => ())
        else ().pure[ConnectionIO]
      } yield ()
    def loop(frontier: Vector[(WorkflowId, WorkflowInstanceKey, Vector[String])]): ConnectionIO[Unit] =
      if (frontier.isEmpty) ().pure[ConnectionIO]
      else
        for {
          _ <- frontier.traverse_ { case (w, kk, ss) => wake(w, kk, ss) }
          next <- frontier.flatTraverse { case (w, kk, ss) => childrenOf(w, kk, ss) }
          _ <- loop(next)
        } yield ()
    for {
      _ <- wake(wf, k, s)
      children <- childrenOf(wf, k, s)
      _ <- loop(children)
    } yield ()
  }

  /** Starts a child workflow: create-if-absent a child instance under the scope
    * derived from the executing parent's identity, generation, and enclosing
    * `Workflow.scoped` path; record the parent relationship, close policy, and
    * inheritance configuration; and schedule the child's first wakeup. Idempotent
    * on parent replay (the replay path touches only the child's own row), and
    * throws [[WorkflowInputConflictException]] when the stored input differs.
    * The child body is never executed here.
    *
    * Fenced on the parent's generation: the transaction first locks the
    * parent's own row (the caller is the parent's run) and requires the current
    * generation to equal the run's, so an orphaned pre-`continueAsNew` run can
    * never attach a stale-generation child after the successor generation — and
    * after its closure record — has committed. The lock order (parent row, then
    * the global event-append lock) matches the terminal transitions.
    */
  override def startChild[In, Out](
      workflow: Workflow[In, Out],
      childKey: WorkflowInstanceKey,
      input: In,
      parentClosePolicy: ParentClosePolicy,
      inheritSignals: SignalInheritance,
      inheritPastEvents: Boolean,
      run: CurrentExecution,
      enclosingScopePath: Vector[String]
  )(using cacheable: Cacheable[In]): WorkflowInstance[In, Out] = {
    val parentId = run.instanceId
    val parentGeneration = run.generation
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

    val existing = runTransaction {
      for {
        fence <- sql"""SELECT generation, terminal_state FROM workflow_instances
                       WHERE workflow_id = ${parentId.workflowId} AND workflow_instance_key = ${parentId.workflowInstanceKey}
                         AND workflow_instance_scope = ${parentId.scope}
                       FOR UPDATE""".query[(Long, Option[String])].option
        _ <- fence match {
          case Some((g, None)) if g == parentGeneration => ().pure[ConnectionIO]
          case _                                         => throw LeaseLostException(parentId)
        }
        // A stale run of the same generation (an orphan that lost its lease
        // after a region restart) must not attach a child into an already
        // discarded interior: refuse when a pending closure of this parent
        // already covers the derived scope. A legitimate new child never
        // matches a pending closure (targets name discarded interiors or old
        // generations), and the parent-row lock above serializes this check
        // against concurrent closures.
        covered <- sql"""SELECT 1 FROM workflow_steps r
                         WHERE r.workflow_id = ${parentId.workflowId} AND r.workflow_instance_key = ${parentId.workflowInstanceKey}
                           AND r.workflow_instance_scope = ${parentId.scope}
                           AND r.step_kind = 'ChildClose'
                           AND $derivedScope[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance
                         LIMIT 1""".query[Int].option
        _ <- if (covered.isDefined) throw LeaseLostException(parentId) else ().pure[ConnectionIO]
        stored <- sql"""SELECT input FROM workflow_instances
                        WHERE workflow_id = $childWorkflowId AND workflow_instance_key = $childKey AND workflow_instance_scope = $derivedScope""".query[String].option
        insertedAndStored <- stored match {
          // Replay: the child exists; touch only the child's own row (no
          // event-append lock, so the replay cannot deadlock against the
          // child's own transactions).
          case Some(s) => (0, Some(s)).pure[ConnectionIO]
          // First creation: the event-append protocol gives the child its
          // inherited-events start sequence id. A raced insert (0 rows) falls
          // back to the replay semantics below.
          case None =>
            for {
              _ <- takeEventAppendLock
              maxSeq <- sql"SELECT COALESCE(MAX(sequence_id), 0) FROM workflow_events".query[Long].unique
              inserted <- sql"""
                INSERT INTO workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope, input, workflow_version_at_creation, generation,
                  parent_workflow_id, parent_instance_key, parent_instance_scope, parent_close_policy,
                  inherit_signals, inherit_past_events, inherited_events_start_sequence_id)
                VALUES ($childWorkflowId, $childKey, $derivedScope, $serializedInput, ${workflow.version}, 0,
                  ${parentId.workflowId}, ${parentId.workflowInstanceKey}, ${parentId.scope}, $policyStr,
                  $inheritSignalsStr, $inheritPastEvents, $maxSeq)
                ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
              """.update.run
              _ <- if (inserted == 1)
                upsertWakeupIO(childWorkflowId, childKey, derivedScope, theClock.instant())
              else ().pure[ConnectionIO]
              raced <- if (inserted == 0)
                sql"""SELECT input FROM workflow_instances
                      WHERE workflow_id = $childWorkflowId AND workflow_instance_key = $childKey AND workflow_instance_scope = $derivedScope""".query[String].option
              else Option.empty[String].pure[ConnectionIO]
            } yield (inserted, raced)
        }
      } yield insertedAndStored
    }

    existing match {
      case (0, Some(stored)) if stored != serializedInput =>
        throw WorkflowInputConflictException(childInstanceId)
      case (0, Some(_)) =>
        runTransaction(updateChildInheritanceIO(childWorkflowId, childKey, derivedScope, inheritSignalsStr, inheritPastEvents))
        WorkflowInstance(workflow, childInstanceId)
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
    runTransaction {
      for {
        row <- sql"""SELECT terminal_state, times_executed, lease_owner, lease_expires_at
                     FROM workflow_instances
                     WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
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
                             WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
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
      scope: Vector[String]
  ): ConnectionIO[Boolean] =
    for {
      steps <- sql"""SELECT 1 FROM workflow_steps
                     WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1""".query[Int].option
      signal <- sql"""SELECT 1 FROM workflow_signal_subscriptions
                      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1""".query[Int].option
      timer <- sql"""SELECT 1 FROM workflow_timer_subscriptions
                     WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1""".query[Int].option
      completion <- sql"""SELECT 1 FROM workflow_completion_subscriptions
                          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1""".query[Int].option
      wakeup <- sql"""SELECT 1 FROM workflow_wakeups
                      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1""".query[Int].option
    } yield steps.isDefined || signal.isDefined || timer.isDefined || completion.isDefined || wakeup.isDefined

  /** Upserts a coalesced wakeup row due immediately, within the caller's
    * transaction.
    */
  private def upsertWakeupIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      now: java.time.Instant
  ): ConnectionIO[Unit] =
    sql"""INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
          VALUES ($workflowId, $key, $scope, $now, $now, 0)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run.map(_ => ())

  /** The guarded `CANCELLED` terminal transition for an instance that has no
    * execution lease (finalized from a cancel request before first start): the
    * guarded status update plus the `WorkflowCompleted(Cancelled)` event, in one
    * transaction. Completion wakeups are produced asynchronously by the job
    * runner. The `Cancelled` outcome carries no user code, so it is encoded
    * directly with the runtime's framing.
    */
  private def finalizeCancelledWithoutLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String]
  ): ConnectionIO[Unit] = {
    val payload = Framing.write("cancelled")
    for {
      updated <- sql"""UPDATE workflow_instances
                       SET terminal_state = 'cancelled', terminal_outcome = $payload, is_accepting_signals = false
                       WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                         AND terminal_state IS NULL""".update.run
      _ <- if (updated == 1)
        for {
          _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
          _ <- appendCompletedEvent(workflowId, key, scope, payload)
          _ <- terminalCleanupIO(workflowId, key, scope)
          _ <- insertChildCloseRowIO(workflowId, key, scope, Vector.empty)
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
      scope: Vector[String]
  ): ConnectionIO[Unit] =
    for {
      _ <- sql"""DELETE FROM workflow_wakeups
                 WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
      _ <- sql"""DELETE FROM workflow_signal_subscriptions
                 WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
      _ <- sql"""DELETE FROM workflow_timer_subscriptions
                 WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
      _ <- sql"""DELETE FROM workflow_completion_subscriptions
                 WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
    } yield ()

  /** Deletes the directly addressed `Signal` events of the instance at a terminal
    * transition. `TimerFired` and `WorkflowCompleted` events are retained
    * (subscribers and replays need them); consumers of the deleted `Signal`
    * events already hold cached Step results.
    */
  private def deleteDirectSignalEventsIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String]
  ): ConnectionIO[Unit] =
    sql"""DELETE FROM workflow_events
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
            AND event_kind = 'Signal'""".update.run.map(_ => ())

  /** Records a pending child close in the parent's own `workflow_steps`: a
    * `ChildClose` row whose `step_scope_within_instance` is the target prefix of
    * the children to close. `Vector.empty` targets every attached child (a
    * terminal parent closes all of them); a non-empty prefix targets exactly the
    * children whose derived scope lies under it, which keeps targeting
    * collision-free across generations and region iterations: children started
    * by a later generation or a later iteration carry markers past the prefix
    * and never match. The row is runtime bookkeeping: every generic step
    * deletion (continueAsNew's generation erase, region-interior erases, fork
    * copies, resets) must exclude `step_kind = 'ChildClose'` rows so pending
    * closures survive. Delivery is asynchronous — the child-close sweep and the
    * run-start self-check apply the policies, and the sweep deletes the row
    * once no attached child matches it — so no parent transaction ever locks
    * another instance's rows.
    *
    * No row is written when no attached child matches the target (the common
    * childless case: terminal transitions of leaf instances, `continueAsNew`
    * and region restarts without children), so bookkeeping stays proportional
    * to actual pending work. The existence check is a read of the parent's own
    * children in the caller's transaction; the `startChild` generation fence
    * guarantees no matching child can attach afterwards.
    */
  private def insertChildCloseRowIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      target: Vector[String]
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      pending <- sql"""SELECT 1 FROM workflow_instances c
                       WHERE c.parent_workflow_id = $workflowId AND c.parent_instance_key = $key AND c.parent_instance_scope = $scope
                         AND c.workflow_instance_scope[1:${target.length}] = $target
                       LIMIT 1""".query[Int].option
      _ <- if (pending.isDefined)
        sql"""INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
              VALUES ($workflowId, $key, $scope, ${s"child-close-${java.util.UUID.randomUUID()}"}, $target, 0, 'ChildClose', 'Pending',
                      ${upickle.default.write(ChildClosePayload(now.toEpochMilli))}, '', NULL, $now, $now)""".update.run
      else ().pure[ConnectionIO]
    } yield ()
  }

  /** The encoded payload of a `ChildClose` row: the creation timestamp of the
    * closure record.
    */
  private case class ChildClosePayload(createdAt: Long) derives upickle.default.ReadWriter

  /** Closes one child per its `ParentClosePolicy` (see [[insertChildCloseRowIO]]
    * and the sweep/self-check paths for the delivery semantics). For every
    * child whose active parent pointer still points at the closing parent, the
    * pointer is cleared (both policies). `Cancel` additionally delivers a
    * cooperative cancellation per child state: a CREATED child (never started,
    * no execution state) is finalized `CANCELLED` immediately; a SUSPENDED or
    * RUNNING child gets `cancel_requested_at` set and, when no live lease
    * exists, a wakeup. `Abandon` clears the pointer only. Idempotent: after the
    * first application the pointer is cleared, so a re-trigger is a no-op, and
    * an already-terminal child is a delivery no-op.
    */
  private def closeOneChildIO(
      child: (WorkflowId, WorkflowInstanceKey, Vector[String], Option[String], Option[String], Int, Option[String], Option[java.time.Instant])
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    val (cwf, ckey, cscope, policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt) = child
    val clearPointer = sql"""UPDATE workflow_instances
                             SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_instance_scope = NULL
                             WHERE workflow_id = $cwf AND workflow_instance_key = $ckey AND workflow_instance_scope = $cscope""".update.run.map(_ => ())
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
                             WHERE workflow_id = $cwf AND workflow_instance_key = $ckey AND workflow_instance_scope = $cscope
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

  /** The child-close delivery sweep (the asynchronous half of
    * [[insertChildCloseRowIO]]): applies every pending closure whose target
    * still matches attached children, and detaches children whose parent row is
    * gone (a hard-deleted parent detaches its children; it never cancels them,
    * matching the behavior of the inheritance walk, which also stops at a
    * missing parent). Candidates are read without locks; each child is closed in
    * its own short transaction that locks only that child's row and re-verifies
    * the condition, so the sweep never locks rows of other instances and is
    * idempotent under concurrent sweeps and run-start self-checks. Finally,
    * `ChildClose` rows that no longer match any attached child are deleted
    * (exhausted records). Returns the number of children closed.
    */
  private[atomicflow] def closePendingChildClosures(limit: Int): Int = {
    val candidates = withDeadlockRetry {
      runTransaction {
        for {
          closable <-
            sql"""SELECT DISTINCT c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
                  FROM workflow_instances c
                  JOIN workflow_steps r
                    ON r.workflow_id = c.parent_workflow_id AND r.workflow_instance_key = c.parent_instance_key
                       AND r.workflow_instance_scope = c.parent_instance_scope
                  WHERE c.parent_workflow_id IS NOT NULL
                    AND r.step_kind = 'ChildClose'
                    AND c.workflow_instance_scope[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance
                  ORDER BY c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
                  LIMIT $limit""".query[(WorkflowId, WorkflowInstanceKey, Vector[String])].to[Vector]
          orphans <-
            sql"""SELECT c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
                  FROM workflow_instances c
                  WHERE c.parent_workflow_id IS NOT NULL
                    AND NOT EXISTS (
                      SELECT 1 FROM workflow_instances p
                      WHERE p.workflow_id = c.parent_workflow_id
                        AND p.workflow_instance_key = c.parent_instance_key
                        AND p.workflow_instance_scope = c.parent_instance_scope)
                  ORDER BY c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
                  LIMIT $limit""".query[(WorkflowId, WorkflowInstanceKey, Vector[String])].to[Vector]
        } yield closable.map(("closable", _)) ++ orphans.map(("orphan", _))
      }
    }
    var closed = 0
    candidates.foreach { case (kind, (wf, k, s)) =>
      if (withDeadlockRetry(runTransaction(closeOnePendingChildIO(wf, k, s, orphan = kind == "orphan")))) closed += 1
    }
    val removed = withDeadlockRetry {
      runTransaction {
        sql"""DELETE FROM workflow_steps r
              WHERE r.step_kind = 'ChildClose'
                AND NOT EXISTS (
                  SELECT 1 FROM workflow_instances c
                  WHERE c.parent_workflow_id = r.workflow_id
                    AND c.parent_instance_key = r.workflow_instance_key
                    AND c.parent_instance_scope = r.workflow_instance_scope
                    AND c.workflow_instance_scope[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance)""".update.run
      }
    }
    val total = closed + removed
    if (total > 0) log.info(s"Child-close sweep closed $closed children and removed $removed exhausted records")
    closed
  }

  /** Closes one sweep candidate in its own transaction, locking only that
    * child's row. Re-verifies under the lock that the parent pointer is still
    * set and — for a closable candidate — that a `ChildClose` row of that parent
    * still matches this child's scope, or — for an orphan candidate — that the
    * parent row is still gone; a raced-away condition makes this a no-op
    * (returns `false`).
    */
  private def closeOnePendingChildIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      orphan: Boolean
  ): ConnectionIO[Boolean] =
    for {
      row <- sql"""SELECT parent_workflow_id, parent_instance_key, parent_instance_scope, parent_close_policy, terminal_state, times_executed, lease_owner, lease_expires_at
                   FROM workflow_instances
                   WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                   FOR UPDATE""".query[
              (Option[String], Option[String], Option[Vector[String]], Option[String], Option[String], Int, Option[String], Option[java.time.Instant])
            ].option
      done <- row match {
        case Some((Some(pwf), Some(pkey), Some(pscope), policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt)) =>
          if (orphan) {
            for {
              stillGone <- sql"""SELECT 1 FROM workflow_instances
                                 WHERE workflow_id = $pwf AND workflow_instance_key = $pkey AND workflow_instance_scope = $pscope""".query[Int].option
              _ <- if (stillGone.isEmpty)
                sql"""UPDATE workflow_instances
                      SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_instance_scope = NULL
                      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
              else ().pure[ConnectionIO]
            } yield stillGone.isEmpty
          } else {
            for {
              matched <- sql"""SELECT 1 FROM workflow_steps r
                               WHERE r.workflow_id = $pwf AND r.workflow_instance_key = $pkey AND r.workflow_instance_scope = $pscope
                                 AND r.step_kind = 'ChildClose'
                                 AND $scope[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance
                               LIMIT 1""".query[Int].option
              _ <- if (matched.isDefined)
                closeOneChildIO((workflowId, key, scope, policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt))
              else ().pure[ConnectionIO]
            } yield matched.isDefined
          }
        case _ => false.pure[ConnectionIO]
      }
    } yield done

  /** The run-start self-check (the synchronous half of [[insertChildCloseRowIO]]
    * delivery): if this instance is a child whose parent has recorded a pending
    * closure matching this instance's scope — or whose parent row is gone —
    * apply the child's own `ParentClosePolicy` now, before the body executes.
    * Runs in one transaction that locks only this instance's own row. Returns
    * `true` only when the run must abort because a never-started child was
    * finalized `CANCELLED` before its first execution; in every other case the
    * run proceeds (a delivered Cancel flag makes the next cancellation
    * checkpoint throw, and `Abandon` merely detaches). A never-started child
    * finalized here records `times_executed = 1` (the run bumps before
    * checking), whereas the same child finalized by the sweep records 0; the
    * terminal outcome is identical and `times_executed` is bookkeeping.
    */
  private def parentClosureSelfCheck(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String]
  ): Boolean = {
    val now = theClock.instant()
    runTransaction {
      for {
        row <- sql"""SELECT times_executed, terminal_state, parent_workflow_id, parent_instance_key, parent_instance_scope, parent_close_policy
                     FROM workflow_instances
                     WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                     FOR UPDATE""".query[
                (Int, Option[String], Option[String], Option[String], Option[Vector[String]], Option[String])
              ].option
        abort <- row match {
          case Some((timesExecuted, None, Some(pwf), Some(pkey), Some(pscope), policy)) =>
            val clearPointer =
              sql"""UPDATE workflow_instances
                    SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_instance_scope = NULL
                    WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
            for {
              parentUsable <- sql"""SELECT 1 FROM workflow_instances p
                                    WHERE p.workflow_id = $pwf AND p.workflow_instance_key = $pkey AND p.workflow_instance_scope = $pscope""".query[Int].option
              result <- parentUsable match {
                // A hard-deleted parent detaches its child; it never cancels.
                case None => clearPointer.as(false)
                case Some(_) =>
                  for {
                    matched <- sql"""SELECT 1 FROM workflow_steps r
                                     WHERE r.workflow_id = $pwf AND r.workflow_instance_key = $pkey AND r.workflow_instance_scope = $pscope
                                       AND r.step_kind = 'ChildClose'
                                       AND $scope[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance
                                     LIMIT 1""".query[Int].option
                    r <- matched match {
                      case None => false.pure[ConnectionIO]
                      case Some(_) =>
                        policy match {
                          case Some("abandon") => clearPointer.as(false)
                          case _ =>
                            if (timesExecuted == 1)
                              // The first execution of a never-started child: the
                              // body never runs; finalize CANCELLED and abort.
                              for {
                                _ <- clearPointer
                                _ <- finalizeCancelledWithoutLease(workflowId, key, scope)
                              } yield true
                            else
                              // A started child: deliver the cooperative flag;
                              // the next cancellation checkpoint throws.
                              for {
                                _ <- clearPointer
                                _ <- sql"""UPDATE workflow_instances SET cancel_requested_at = $now
                                           WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                                             AND cancel_requested_at IS NULL""".update.run
                              } yield false
                        }
                    }
                  } yield r
              }
            } yield result
          case _ => false.pure[ConnectionIO]
        }
      } yield abort
    }
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
    val exists = runTransaction {
      sql"""SELECT 1 FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".query[Int].option
    }
    if (exists.isEmpty)
      throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
    escalateTerminated(instanceId)
    ()
  }

  /** The guarded `continueAsNew` in-place transition, committed in ONE
    * transaction under the run's lease: erase the old generation's execution
    * records (Step rows — except pending `ChildClose` records — subscriptions,
    * and wakeup), keep the exact-key signal cursors, record a `ChildClose` row
    * targeting exactly the children of the closing generation (delivery is
    * asynchronous; see [[insertChildCloseRowIO]]), delete the directly
    * addressed `Signal` events, increment the generation, install the new
    * input, re-open signal acceptance for the successor, and upsert its wakeup.
    * Fenced: if the lease no longer belongs to this run, no rows are affected
    * and [[LeaseLostException]] is thrown.
    */
  private def continueAsNewTransition(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      worker: String,
      token: Long,
      newSerializedInput: String
  ): Unit = {
    val now = theClock.instant()
    val res = runTransaction {
      for {
        generation <- sql"""SELECT generation FROM workflow_instances
                    WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                    FOR UPDATE""".query[Long].unique
        fenceOk <- sql"""SELECT 1 FROM workflow_instances
                         WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                           AND lease_owner = $worker AND fencing_token = $token""".query[Int].option
        done <- fenceOk match {
          case None => false.pure[ConnectionIO]
          case Some(_) =>
            for {
              _ <- sql"""DELETE FROM workflow_steps
                         WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                           AND step_kind <> 'ChildClose'""".update.run
              _ <- terminalCleanupIO(workflowId, key, scope)
              _ <- insertChildCloseRowIO(workflowId, key, scope,
                                         scope ++ Vector(workflowId, s"$key@$generation"))
              _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
              _ <- sql"""UPDATE workflow_instances
                         SET input = $newSerializedInput, generation = generation + 1, is_accepting_signals = true
                         WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
              _ <- upsertWakeupIO(workflowId, key, scope, now)
            } yield true
        }
      } yield done
    }
    if (!res) throw LeaseLostException(WorkflowInstanceId(workflowId, key, scope))
    ()
  }

  /** The guarded `TERMINATED` terminal transition, shared by the public
    * [[terminate]] and the cancellation-escalation sweep. Row-locks the
    * instance; a missing or already-terminal instance is a no-op (returns
    * `false`); otherwise the guarded status update plus the
    * `WorkflowCompleted(Terminated)` event, lease revocation (fencing-token
    * bump and clearing owner/expiry), and terminal cleanup commit in one
    * transaction. Completion wakeups are produced asynchronously by the job
    * runner. Idempotent: a concurrent winner makes a later caller a no-op.
    */
  private[atomicflow] def escalateTerminated(instanceId: WorkflowInstanceId): Boolean = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val payload = Framing.write("terminated")
    runTransaction {
      for {
        terminal <- sql"""SELECT terminal_state FROM workflow_instances
                          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
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
                         WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                           AND terminal_state IS NULL""".update.run
              _ <- if (u == 1)
                for {
                  _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
                  _ <- appendCompletedEvent(workflowId, key, scope, payload)
                  _ <- terminalCleanupIO(workflowId, key, scope)
                  _ <- insertChildCloseRowIO(workflowId, key, scope, Vector.empty)
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
      timerId: java.util.UUID,
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String]
  ): ConnectionIO[Boolean] = {
    val now = theClock.instant()
    for {
      locked <- sql"""SELECT deadline FROM workflow_timer_subscriptions
                      WHERE timer_id = $timerId
                      FOR UPDATE""".query[java.time.Instant].option
      fired <- locked match {
        case None => false.pure[ConnectionIO]
        case Some(_) =>
          for {
            exists <- sql"""SELECT 1 FROM workflow_events
                            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                              AND event_kind = 'TimerFired' AND event_key = ${timerId.toString}""".query[Int].option
            _ <- if (exists.isEmpty)
              for {
                _ <- appendEvent(workflowId, key, scope, "TimerFired", timerId.toString, "")
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
    runTransaction {
      for {
        updated <- sql"""UPDATE workflow_instances
                         SET lease_owner = NULL, lease_expires_at = NULL
                         WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                           AND lease_owner IS NOT NULL AND terminal_state IS NULL""".update.run
        _ <- if (updated == 1) upsertWakeupIO(workflowId, key, scope, now)
             else ().pure[ConnectionIO]
      } yield updated == 1
    }
  }

  /** Best-effort watermark dispatch. Processes events appended after the stored
    * watermark and upserts the wakeups they imply. At most one dispatcher works
    * at a time: it locks the single watermark row with FOR UPDATE SKIP LOCKED and
    * a concurrent caller returns immediately. TimerFired events are skipped here
    * because the timer sweep already wakes their owning instance. Returns the
    * number of events processed.
    */
  private[atomicflow] def dispatchNewEvents(batchSize: Int): Int =
    withDeadlockRetry {
      runTransaction {
        sql"SELECT dispatched_through FROM workflow_event_dispatch WHERE id = 1 FOR UPDATE SKIP LOCKED"
          .query[Long]
          .option
          .flatMap {
            case None => 0.pure[ConnectionIO]
            case Some(through) =>
              sql"""SELECT sequence_id, event_kind, workflow_id, workflow_instance_key, workflow_instance_scope, event_key
                    FROM workflow_events WHERE sequence_id > $through
                    ORDER BY sequence_id LIMIT $batchSize""".query[
                  (Long, String, WorkflowId, WorkflowInstanceKey, Vector[String], String)
                ].to[Vector].flatMap { events =>
                  events.traverse_ { case (_, kind, wf, k, s, ek) =>
                    kind match {
                      case "Signal" =>
                        wakeMatchingSubscribers(wf, k, s, ek) *> wakeInheritingDescendantsIO(wf, k, s, ek)
                      case "WorkflowCompleted" =>
                        wakeCompletionSubscribers(wf, k, s)
                      case _ => ().pure[ConnectionIO]
                    }
                  } *> events.lastOption.traverse_ { case (seq, _, _, _, _, _) =>
                    sql"UPDATE workflow_event_dispatch SET dispatched_through = $seq WHERE id = 1".update.run
                  }.map(_ => events.size)
                }
          }
      }
    }

  /** Full liveness scan. Wakes every non-terminal instance holding a pending
    * signal or completion subscription that has a visible, unconsumed candidate.
    * This is the correctness guarantee behind the asynchronous wakeup jobs; the
    * watermark dispatcher is only a latency optimization. Returns the number of
    * wakeups inserted. Timer and update subscriptions are excluded (the timer
    * sweep and the instance-row lock cover them).
    */
  private[atomicflow] def scanAllSubscriptions(): Int = {
    val now = theClock.instant()
    withDeadlockRetry {
      runTransaction {
        for {
          direct <- sql"""INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
              SELECT s.workflow_id, s.workflow_instance_key, s.workflow_instance_scope, $now, $now, 0
              FROM workflow_signal_subscriptions s
              JOIN workflow_instances i
                ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
              WHERE i.terminal_state IS NULL
                AND EXISTS (
                  SELECT 1 FROM workflow_events e
                  WHERE e.event_kind = 'Signal' AND e.event_key = s.signal_key
                    AND e.workflow_id = s.workflow_id AND e.workflow_instance_key = s.workflow_instance_key AND e.workflow_instance_scope = s.workflow_instance_scope
                    AND e.sequence_id > COALESCE((
                      SELECT c.sequence_id FROM signal_cursor c
                      WHERE c.workflow_id = s.workflow_id AND c.workflow_instance_key = s.workflow_instance_key
                        AND c.workflow_instance_scope = s.workflow_instance_scope AND c.signal_key = s.signal_key), 0)
                )
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run
          inherited <- sql"""WITH RECURSIVE ancestry AS (
                SELECT s.workflow_id AS sub_wf, s.workflow_instance_key AS sub_key, s.workflow_instance_scope AS sub_scope, s.signal_key AS signal_key,
                       i.workflow_id AS node_wf, i.workflow_instance_key AS node_key, i.workflow_instance_scope AS node_scope,
                       i.inherit_signals AS node_policy,
                       i.parent_workflow_id AS parent_wf, i.parent_instance_key AS parent_key, i.parent_instance_scope AS parent_scope,
                       i.inherited_events_start_sequence_id AS start_seq, i.inherit_past_events AS past, 0 AS depth
                FROM workflow_signal_subscriptions s
                JOIN workflow_instances i
                  ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
                WHERE i.terminal_state IS NULL
                  AND i.parent_workflow_id IS NOT NULL
                UNION ALL
                SELECT a.sub_wf, a.sub_key, a.sub_scope, a.signal_key,
                       p.workflow_id, p.workflow_instance_key, p.workflow_instance_scope,
                       p.inherit_signals, p.parent_workflow_id, p.parent_instance_key, p.parent_instance_scope,
                       a.start_seq, a.past, a.depth + 1
                FROM ancestry a
                JOIN workflow_instances p
                  ON p.workflow_id = a.parent_wf AND p.workflow_instance_key = a.parent_key AND p.workflow_instance_scope = a.parent_scope
                WHERE a.parent_wf IS NOT NULL
                  AND a.depth < 64
                  AND p.terminal_state IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM workflow_steps r
                    WHERE r.workflow_id = p.workflow_id AND r.workflow_instance_key = p.workflow_instance_key AND r.workflow_instance_scope = p.workflow_instance_scope
                      AND r.step_kind = 'ChildClose'
                      AND a.node_scope[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance)
                  AND ( a.node_policy = 'all'
                        OR ( a.node_policy LIKE 'some:%'
                             AND EXISTS (
                               SELECT 1 FROM jsonb_array_elements_text(substring(a.node_policy from 6)::jsonb) AS t(pfx)
                               WHERE left(a.signal_key, length(pfx)) = pfx
                             ) ) )
              )
              INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
              SELECT DISTINCT a.sub_wf, a.sub_key, a.sub_scope, $now, $now, 0
              FROM ancestry a
              WHERE a.depth > 0
                AND EXISTS (
                  SELECT 1 FROM workflow_events e
                  WHERE e.event_kind = 'Signal' AND e.event_key = a.signal_key
                    AND e.workflow_id = a.node_wf AND e.workflow_instance_key = a.node_key AND e.workflow_instance_scope = a.node_scope
                    AND e.sequence_id > COALESCE((
                      SELECT c.sequence_id FROM signal_cursor c
                      WHERE c.workflow_id = a.sub_wf AND c.workflow_instance_key = a.sub_key
                        AND c.workflow_instance_scope = a.sub_scope AND c.signal_key = a.signal_key), 0)
                    AND (a.past OR a.start_seq IS NULL OR e.sequence_id > a.start_seq)
                )
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run
          completion <- sql"""INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
              SELECT s.workflow_id, s.workflow_instance_key, s.workflow_instance_scope, $now, $now, 0
              FROM workflow_completion_subscriptions s
              JOIN workflow_instances i
                ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
              WHERE i.terminal_state IS NULL
                AND EXISTS (
                  SELECT 1 FROM workflow_events e
                  WHERE e.event_kind = 'WorkflowCompleted'
                    AND e.event_key = ''
                    AND e.workflow_id = s.completed_workflow_id
                    AND e.workflow_instance_key = s.completed_workflow_instance_key
                    AND e.workflow_instance_scope = s.completed_workflow_instance_scope
                )
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run
        } yield direct + inherited + completion
      }
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

        val row = runTransaction {
          sql"""SELECT terminal_state, terminal_outcome, input, workflow_version_at_creation, generation
                FROM workflow_instances
                WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".query[
              (Option[String], Option[String], String, Long, Long)
            ].option
        }

        val (terminalState, terminalOutcome, _, _, _) = row match {
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
              val bumped = runTransaction {
                sql"""UPDATE workflow_instances
                      SET times_executed = times_executed + 1, last_run_at = $now
                      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                        AND lease_owner = $worker AND fencing_token = $token""".update.run
              }
              if (bumped != 1) throw LeaseLostException(instanceId)

              // Re-read the instance row now that we hold the lease. A caller-thread
              // run may have waited out a live lease, during which a concurrent
              // continueAsNew/reset replaced the input and bumped the generation; the
              // pre-acquisition read above would then be stale. Reading fenced by our
              // owner+token guarantees we observe the current row (or detect we lost
              // the lease). The generation, input, version, and terminal state below
              // all come from this fresh read.
              val freshRow = runTransaction {
                sql"""SELECT terminal_state, terminal_outcome, input, workflow_version_at_creation, generation, parent_workflow_id
                      FROM workflow_instances
                      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                        AND lease_owner = $worker AND fencing_token = $token""".query[
                    (Option[String], Option[String], String, Long, Long, Option[String])
                  ].option
              }
              val (freshTerminal, freshOutcome, freshInputSerialized, freshVersionAtCreation, freshGeneration, freshParent) =
                freshRow match {
                  case Some(r) => r
                  case None    => throw LeaseLostException(instanceId)
                }

              freshTerminal match {
                case Some(state) =>
                  handleTerminalRead(state, freshOutcome, outCacheable, completionCodec, instanceId)
                case None =>
                  // Run-start self-check: a child whose parent has recorded a
                  // pending close (or is gone) applies its own policy before the
                  // body executes. Aborts the run only for a never-started child
                  // finalized CANCELLED; otherwise the run proceeds and the
                  // delivered Cancel flag makes the next checkpoint throw. The
                  // fresh parent pointer is a cheap filter: it only ever goes
                  // from set to cleared, so a top-level instance skips the extra
                  // transaction.
                  if (freshParent.isDefined && parentClosureSelfCheck(workflowId, key, scope)) {
                    log.info(s"Workflow instance $instanceId cancelled by pending parent close before first execution")
                    WorkflowRunResult.WorkflowCancelled
                  } else {
                    val input = wf.inputCacheable.read(freshInputSerialized)
                    val ctxInstanceId = instanceId
                    val ctxVersionAtCreation = freshVersionAtCreation
                    val ctxExecution = new PostgresCurrentExecution(worker, token, workflowId, key, scope, freshGeneration)
                    val ctx = new WorkflowContext(
                      ctxInstanceId,
                      ctxVersionAtCreation,
                      PostgresWorkflowRuntime.this
                    )(ctxExecution)

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
                      case e: ContinueAsNewException =>
                        val newInput = wf.inputCacheable.read(e.encoded)
                        runUnconsumedSignals(wf, workflowId, key, scope, worker, token)
                        val newSerializedInput = wf.inputCacheable.write(newInput)
                        continueAsNewTransition(workflowId, key, scope, worker, token, newSerializedInput)
                        log.info(s"Workflow instance $instanceId continued as new")
                        return WorkflowRunResult.ContinueAsNew
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
                  }
              }
            } finally {
              releaseLeaseIfStillOwned(instanceId, worker, token)
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
      scope: Vector[String],
      worker: String,
      leaseDuration: FiniteDuration
  ): ConnectionIO[Option[Long]] = {
    val now = theClock.instant()
    val expires = leaseExpiry(now, leaseDuration)
    sql"""UPDATE workflow_instances
          SET lease_owner = $worker, fencing_token = fencing_token + 1, lease_expires_at = $expires
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
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
      scope: Vector[String],
      worker: String,
      leaseDuration: FiniteDuration,
      leaseAcquireTimeout: FiniteDuration
  ): Option[Long] = {
    val pollIntervalMillis = 100L
    val deadlineNanos = System.nanoTime() + leaseAcquireTimeout.toNanos
    var acquired: Option[Long] = None
    while (acquired.isEmpty && System.nanoTime() < deadlineNanos) {
      acquired = runTransaction(tryAcquireOnceIO(workflowId, key, scope, worker, leaseDuration))
      if (acquired.isEmpty && System.nanoTime() < deadlineNanos) Thread.sleep(pollIntervalMillis)
    }
    acquired
  }

  /** Clears the lease if it is still owned by this exact incarnation — the run
    * or claim holding `worker`/`token`. "Owned" means the row still records
    * that worker at that fencing token: if the lease expired and another
    * executor took over (bumping the token), this releases nothing and cannot
    * clobber the new owner's lease. Optimization only — correctness relies on
    * expiry plus fencing, not on this release. The run path calls it in a
    * `finally` so a fault-injected abort that never reached the run's own
    * release cannot leak a lease.
    */
  private[atomicflow] def releaseLeaseIfStillOwned(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Unit =
    runTransaction {
      sql"""UPDATE workflow_instances
            SET lease_owner = NULL, lease_expires_at = NULL
            WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey} AND workflow_instance_scope = ${instanceId.scope}
              AND lease_owner = $worker AND fencing_token = $token""".update.run
    }

  /** The guarded terminal transition, atomic with the `WorkflowCompleted` event
    * append and the deletion of this instance's directly addressed `Signal`
    * events: the guarded `terminal_state`/`terminal_outcome` update, the event
    * append, and the `Signal` deletion commit together in one transaction.
    * Completion wakeups are produced asynchronously by the job runner. Wins only
    * if the instance is not already terminal and the lease is still ours.
    * `TimerFired` and `WorkflowCompleted` events are retained (subscribers and
    * replays need them); consumers of the deleted `Signal` events already hold
    * cached Step results.
    *
    * Returns rows updated (1 if this writer won the transition, 0 if another
    * writer already made it terminal).
    */
  private def terminalTransitionAndEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      worker: String,
      token: Long,
      state: String,
      payload: String
  ): Int =
    runTransaction {
      for {
        updated <- sql"""UPDATE workflow_instances
              SET terminal_state = $state, terminal_outcome = $payload, is_accepting_signals = false
              WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                AND terminal_state IS NULL AND lease_owner = $worker AND fencing_token = $token""".update.run
        _ <- if (updated == 1)
          for {
            _ <- deleteDirectSignalEventsIO(workflowId, key, scope)
            _ <- appendCompletedEvent(workflowId, key, scope, payload)
            _ <- terminalCleanupIO(workflowId, key, scope)
            _ <- insertChildCloseRowIO(workflowId, key, scope, Vector.empty)
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
      scope: Vector[String],
      worker: String,
      token: Long
  ): Unit =
    runTransaction {
      sql"""UPDATE workflow_instances SET is_accepting_signals = false
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
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
      scope: Vector[String]
  ): Map[SignalKey, Seq[String]] =
    runTransaction {
      sql"""SELECT event_key, payload FROM workflow_events
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
              AND event_kind = 'Signal'
              AND sequence_id > (
                SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
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
      scope: Vector[String],
      worker: String,
      token: Long
  ): Unit = {
    setNotAcceptingSignals(workflowId, key, scope, worker, token)
    val unconsumed = gatherUnconsumedSignals(workflowId, key, scope)
    workflow.onUnconsumedSignals(unconsumed)
  }

  /** Upserts the wakeup of every non-terminal instance holding a pending
    * completion subscription on the instance that just completed, `ON CONFLICT
    * DO NOTHING` with `scheduled_at = now`. Called by the best-effort watermark
    * dispatcher when it processes a committed `WorkflowCompleted` event; the
    * full subscription scan covers the same case independently.
    */
  private def wakeCompletionSubscribers(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String]
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      subscribers <- sql"""SELECT s.workflow_id, s.workflow_instance_key, s.workflow_instance_scope
            FROM workflow_completion_subscriptions s
            JOIN workflow_instances i
              ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
            WHERE s.completed_workflow_id = $workflowId AND s.completed_workflow_instance_key = $key AND s.completed_workflow_instance_scope = $scope
              AND i.terminal_state IS NULL""".query[
          (WorkflowId, WorkflowInstanceKey, Vector[String])
        ].to[Vector]
      _ <- subscribers.traverse_ { case (subWf, subKey, subScope) =>
        sql"""
          INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
          VALUES ($subWf, $subKey, $subScope, $now, $now, 0)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
        """.update.run.map(_ => ())
      }
    } yield ()
  }

  /** Appends a `WorkflowCompleted` event via the global event-append protocol.
    */
  private def appendCompletedEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
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
      scope: Vector[String],
      kind: String,
      eventKey: String,
      payload: String
  ): ConnectionIO[Unit] = {
    val createdAt = theClock.instant()
    for {
      _ <- takeEventAppendLock
      sequenceId <- sql"SELECT nextval('workflow_event_sequence')".query[Long].unique
      _ <- sql"""
        INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, workflow_instance_key, workflow_instance_scope, event_key, payload, created_at)
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
    * signal's [[Cacheable]]). The sender only appends; wakeups are produced
    * asynchronously by the job runner. Does not acquire the execution lease.
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
    runTransaction {
      for {
        accepting <- sql"""SELECT is_accepting_signals FROM workflow_instances
              WHERE workflow_id = $workflowId AND workflow_instance_key = $instanceKey AND workflow_instance_scope = $scope
              FOR UPDATE""".query[Boolean].option
        result <- accepting match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $workflowInstanceId")
          case Some(false) => SignalSendResult.InstanceAlreadyCompleted.pure[ConnectionIO]
          case Some(true) =>
            for {
              _ <- appendEvent(workflowId, instanceKey, scope, "Signal", key, payload)
            } yield SignalSendResult.Success
        }
      } yield result
    }
  }

  /** Upserts the addressed instance's own wakeup row when it holds a pending
    * subscription for the exact signal key and is still non-terminal. Called by
    * the best-effort watermark dispatcher; the sender no longer upserts wakeups,
    * and no payload filter is evaluated (only key-matching subscriptions matter).
    */
  private def wakeMatchingSubscribers(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      signalKey: SignalKey
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      has <- sql"""SELECT 1
            FROM workflow_signal_subscriptions s
            JOIN workflow_instances i
              ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
            WHERE s.workflow_id = $workflowId AND s.workflow_instance_key = $key AND s.workflow_instance_scope = $scope AND s.signal_key = $signalKey
              AND i.terminal_state IS NULL
            LIMIT 1""".query[Int].option
      _ <- if (has.isDefined)
        sql"""
          INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
          VALUES ($workflowId, $key, $scope, $now, $now, 0)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
        """.update.run.map(_ => ())
      else ().pure[ConnectionIO]
    } yield ()
  }

  /** Upserts a wakeup for every currently eligible, non-terminal descendant of
    * the addressed instance that is suspended on `signalKey`, so one committed
    * send reaches all descendants that inherit it. A descendant is eligible when
    * every parent-child edge on the path from the addressed instance down
    * currently permits the key (evaluated at dispatch time; the awaiting
    * descendant re-checks eligibility when it runs). Waking a superset is
    * harmless because the run-time candidate query applies the policy again.
    * Called by the best-effort watermark dispatcher; the full subscription scan
    * covers the same case independently.
    */
  private def wakeInheritingDescendantsIO(
      wf: WorkflowId,
      k: WorkflowInstanceKey,
      s: Vector[String],
      signalKey: SignalKey
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    def childrenOf(
        w: WorkflowId,
        kk: WorkflowInstanceKey,
        ss: Vector[String]
    ): ConnectionIO[Vector[(WorkflowId, WorkflowInstanceKey, Vector[String], Option[String])]] =
      sql"""SELECT workflow_id, workflow_instance_key, workflow_instance_scope, inherit_signals FROM workflow_instances
            WHERE parent_workflow_id = $w AND parent_instance_key = $kk AND parent_instance_scope = $ss""".query[
          (WorkflowId, WorkflowInstanceKey, Vector[String], Option[String])
        ].to[Vector]
    def wakeIfSubscribed(w: WorkflowId, kk: WorkflowInstanceKey, ss: Vector[String]): ConnectionIO[Unit] =
      for {
        has <- sql"""SELECT 1
                     FROM workflow_signal_subscriptions s
                     JOIN workflow_instances i
                       ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
                     WHERE s.workflow_id = $w AND s.workflow_instance_key = $kk AND s.workflow_instance_scope = $ss AND s.signal_key = $signalKey
                       AND i.terminal_state IS NULL
                     LIMIT 1""".query[Int].option
        _ <- if (has.isDefined)
          sql"""INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
                VALUES ($w, $kk, $ss, $now, $now, 0)
                ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING""".update.run.map(_ => ())
        else ().pure[ConnectionIO]
      } yield ()
    def loop(frontier: Vector[(WorkflowId, WorkflowInstanceKey, Vector[String])]): ConnectionIO[Unit] =
      if (frontier.isEmpty) ().pure[ConnectionIO]
      else
        for {
          _ <- frontier.traverse_ { case (w, kk, ss) => wakeIfSubscribed(w, kk, ss) }
          next <- frontier.flatTraverse { case (w, kk, ss) =>
            childrenOf(w, kk, ss).map(_.collect {
              case (cw, ck, cs, policy) if inheritancePermits(decodeSignalInheritance(policy), signalKey) => (cw, ck, cs)
            })
          }
          _ <- loop(next)
        } yield ()
    for {
      children <- childrenOf(wf, k, s).map(_.collect {
        case (cw, ck, cs, policy) if inheritancePermits(decodeSignalInheritance(policy), signalKey) => (cw, ck, cs)
      })
      _ <- loop(children)
    } yield ()
  }

  /** Outcomes of the terminal/dedup preparation phase of `sendUpdate`. */
  private sealed trait SendOutcome
  private object SendOutcome {
    case object InstanceCompleted extends SendOutcome
    final case class AlreadyHandled(encodedResult: String) extends SendOutcome
    case object Ready extends SendOutcome
  }

  /** Sanity cap for the sender's wait on a live lease before retrying. */
  private val SendLeaseWaitCap: FiniteDuration = 60.seconds

  /** The lease poll interval of `sendUpdate`'s wait-for-expiry loop. */
  private val SendLeasePollIntervalMillis: Long = 50L

  /** Sends a synchronous [[Update]] to a single instance and blocks for its
    * outcome.
    *
    * In one transaction, checks the instance (missing → throws
    * [[WorkflowNotFoundException]], terminal → `InstanceAlreadyCompleted`) and
    * deduplicates on `idempotencyKey` (a matching handled record → return its
    * `Success` without running; a matching pending record → reused). If the
    * instance is free it then runs the workflow on the sender's thread (the
    * record already exists, so an `awaitUpdate` in the run handles it); if the
    * instance is leased by another owner it waits for the lease to expire and
    * retries from the beginning. After the run, a handled record yields
    * `Success`; otherwise `Unhandled` (and, unless `persistUnhandledUpdates`,
    * the record is deleted).
    */
  override def sendUpdate[I, R](
      workflow: Workflow[?, ?],
      workflowInstanceId: WorkflowInstanceId,
      updateKey: String,
      input: I,
      idempotencyKey: String = "",
      persistUnhandledUpdates: Boolean = false
  )(using u: atomicflow.Update[I, R], cacheableThrowable: Cacheable[Throwable]): UpdateSendResult[R] = {
    val wf = workflowInstanceId.workflowId
    val k = workflowInstanceId.workflowInstanceKey
    val s = workflowInstanceId.scope
    val encodedInput = u.inputCacheable.write(input)
    val respCacheable = u.responseCacheable

    def notFound: WorkflowNotFoundException =
      new WorkflowNotFoundException(s"Workflow instance not found: $workflowInstanceId")

    def readOutcome(idem: String): ConnectionIO[SendOutcome] =
      for {
        exists <- sql"""SELECT 1 FROM workflow_instances
                        WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s
                        FOR UPDATE""".query[Int].option
        outcome <- exists match {
          case None => throw notFound
          case Some(_) =>
            for {
              terminal <- sql"""SELECT terminal_state FROM workflow_instances
                                WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s""".query[Option[String]].unique
              existingIdem <- if (idem.isEmpty) Option.empty[(java.time.Instant, Option[String])].pure[ConnectionIO]
                              else
                                sql"""SELECT created_at, result FROM workflow_updates
                                      WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s
                                        AND update_key = $updateKey AND idempotency_key = $idem
                                      ORDER BY created_at""".query[(java.time.Instant, Option[String])].option
              res <- existingIdem match {
                case Some((_, Some(encodedResult))) => SendOutcome.AlreadyHandled(encodedResult).pure[ConnectionIO]
                case _ if terminal.isDefined        => SendOutcome.InstanceCompleted.pure[ConnectionIO]
                case _                              => SendOutcome.Ready.pure[ConnectionIO]
              }
            } yield res
        }
      } yield outcome

    /** Ensures a `workflow_updates` record exists for this send and returns its
      * identity (`createdAt`, `idempotencyKey`). Reuses a pending record when
      * `idem` is non-empty and one already exists; otherwise inserts a fresh
      * record. Both paths `RETURNING created_at` so the caller keys on the
      * record's durable identity rather than a Java `Instant` it generated (PG
      * stores micros), and the non-empty path is `ON CONFLICT`-safe so two
      * concurrent same-key senders join the winner instead of racing the
      * SELECT→INSERT gap into a raw unique violation. Called only after the
      * lease is confirmed free, so the retry-from-the-beginning lease wait never
      * inserts duplicates.
      */
    def ensureRecord(idem: String): (java.time.Instant, String) = {
      val now = theClock.instant()
      val createdAt =
        if (idem.isEmpty)
          runTransaction {
            sql"""INSERT INTO workflow_updates
                    (workflow_id, workflow_instance_key, workflow_instance_scope, update_key, encoded_input, idempotency_key, created_at, updated_at)
                  VALUES ($wf, $k, $s, $updateKey, $encodedInput, $idem, $now, $now)
                  RETURNING created_at""".query[java.time.Instant].unique
          }
        else
          runTransaction {
            sql"""INSERT INTO workflow_updates
                    (workflow_id, workflow_instance_key, workflow_instance_scope, update_key, encoded_input, idempotency_key, created_at, updated_at)
                  VALUES ($wf, $k, $s, $updateKey, $encodedInput, $idem, $now, $now)
                  ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, update_key, idempotency_key) WHERE idempotency_key <> ''
                    DO UPDATE
                    SET updated_at = workflow_updates.updated_at
                  RETURNING created_at""".query[java.time.Instant].unique
          }
      (createdAt, idem)
    }

    def isLeaseHeld: Boolean =
      runTransaction {
        sql"""SELECT lease_owner IS NOT NULL AND lease_expires_at > ${theClock.instant()}
              FROM workflow_instances
              WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s""".query[Boolean].unique
      }

    def waitForLeaseExpiry(): Unit = {
      val deadline = System.nanoTime() + SendLeaseWaitCap.toNanos
      var held = true
      while (held && System.nanoTime() < deadline) {
        held = isLeaseHeld
        if (held) Thread.sleep(SendLeasePollIntervalMillis)
      }
    }

    def upsertWakeupForUpdate(): Unit =
      runTransaction {
        for {
          has <- sql"""SELECT 1 FROM workflow_update_subscriptions
                       WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s AND update_key = $updateKey
                       LIMIT 1""".query[Int].option
          _ <- if (has.isDefined) upsertWakeupIO(wf, k, s, theClock.instant()) else ().pure[ConnectionIO]
        } yield ()
      }

    def readResult(createdAt: java.time.Instant, idem: String): Option[R] =
      runTransaction {
        sql"""SELECT result FROM workflow_updates
              WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s
                AND update_key = $updateKey AND idempotency_key = $idem AND created_at = $createdAt""".query[
            Option[String]
          ].option
      }.flatten.map(respCacheable.read)

    def deleteUnhandled(createdAt: java.time.Instant, idem: String): Unit =
      runTransaction {
        sql"""DELETE FROM workflow_updates
              WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s
                AND update_key = $updateKey AND idempotency_key = $idem AND created_at = $createdAt
                AND handled_at IS NULL""".update.run
      }

    /** Deletes the record by durable identity regardless of `handled_at`. Used to
      * retire a handled record with an empty `idempotency_key`, which serves no
      * future dedup purpose. Records are identity-unique by `created_at`, so this
      * only ever touches this send's own row.
      */
    def deleteRecord(createdAt: java.time.Instant, idem: String): Unit =
      runTransaction {
        sql"""DELETE FROM workflow_updates
              WHERE workflow_id = $wf AND workflow_instance_key = $k AND workflow_instance_scope = $s
                AND update_key = $updateKey AND idempotency_key = $idem AND created_at = $createdAt""".update.run
      }

    var done = false
    var result: UpdateSendResult[R] = null.asInstanceOf[UpdateSendResult[R]]
    while (!done) {
      val outcome = runTransaction(readOutcome(idempotencyKey))
      outcome match {
        case SendOutcome.InstanceCompleted =>
          result = UpdateSendResult.InstanceAlreadyCompleted
          done = true
        case SendOutcome.AlreadyHandled(encodedResult) =>
          result = UpdateSendResult.Success(respCacheable.read(encodedResult))
          done = true
        case SendOutcome.Ready =>
          if (isLeaseHeld) {
            waitForLeaseExpiry()
          } else {
            val (createdAt, idem) = ensureRecord(idempotencyKey)
            upsertWakeupForUpdate()
            val runSucceeded = try {
              runWorkflowInstance(workflow, workflowInstanceId)(using cacheableThrowable)
              true
            } catch {
              case _: LeaseUnavailableException =>
                if (idem.isEmpty) deleteUnhandled(createdAt, idem)
                false
            }
            if (runSucceeded) {
              readResult(createdAt, idem) match {
                case Some(value) =>
                  if (idem.isEmpty) deleteRecord(createdAt, idem)
                  result = UpdateSendResult.Success(value)
                  done = true
                case None =>
                  if (!persistUnhandledUpdates) deleteUnhandled(createdAt, idem)
                  result = UpdateSendResult.Unhandled
                  done = true
              }
            }
          }
      }
    }
    result
  }

  private def readTerminalAndReturn[Out](
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: Vector[String],
      outCacheable: Cacheable[Out],
      completionCodec: Cacheable[WorkflowCompletionResult[Out]],
      instanceId: WorkflowInstanceId
  ): WorkflowRunResult[Out] = {
    val (state, outcome) = runTransaction {
      sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".query[(String, Option[String])].unique
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

  override def readStep(
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
      scope: Vector[String],
      stepId: String,
      stepScopePath: Vector[String],
      stepVersion: Long
  ): Option[StoredStep] =
    runTransaction {
      sql"""SELECT state_kind, state_payload, input_fingerprints, expires_at FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
              AND step_id = $stepId AND step_scope_within_instance = $stepScopePath AND step_version = $stepVersion""".query[
          (String, String, String, Option[java.time.Instant])
        ].option
    }.map { case (kind, payload, fingerprints, expiresAt) =>
      StoredStep(StoredStep.State.fromString(kind), payload, fingerprints, expiresAt)
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
      Option[Vector[String]],
      java.time.Instant,
      Option[java.time.Instant],
      Int,
      Option[String]
    )

  private val InfoSelect: Fragment =
    fr"""SELECT workflow_instance_key, workflow_version_at_creation, generation,
         parent_workflow_id, parent_instance_key, parent_instance_scope, created_at,
         last_run_at, times_executed, terminal_state
         FROM workflow_instances"""

  private def terminalEnum(state: String): WorkflowTerminalState = state match {
    case "completed"  => WorkflowTerminalState.Completed
    case "failed"     => WorkflowTerminalState.Failed
    case "cancelled"  => WorkflowTerminalState.Cancelled
    case "terminated" => WorkflowTerminalState.Terminated
    case other        => throw new StepSerializationFailed(s"Unknown terminal state '$other'")
  }

  private def toInfo(workflowId: WorkflowId, scope: Vector[String], r: InfoRow): WorkflowInstance.Info = {
    val (key, versionAtCreation, generation, parentWf, parentKey, parentScope, createdAt, lastRunAt, timesExecuted, terminal) = r
    val parentId = parentWf.map(pwf => WorkflowInstanceId(pwf, parentKey.getOrElse(""), parentScope.getOrElse(Vector.empty)))
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
      scope: Vector[String] = Vector.empty
  ): Vector[WorkflowInstance.Info] = {
    val pattern = likeEscaped(keyPrefix) + "%"
    runTransaction {
      (InfoSelect ++ fr"WHERE workflow_id = $workflowId AND workflow_instance_scope = $scope AND workflow_instance_key LIKE $pattern ESCAPE '\' ORDER BY workflow_instance_key")
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
    runTransaction {
      (InfoSelect ++ fr"WHERE workflow_id = $workflowId AND workflow_instance_scope = '{}' AND terminal_state IS NULL " ++
        waitingCond ++ fr"ORDER BY workflow_instance_key " ++ limitFrag).query[InfoRow].to[Vector]
    }.map(toInfo(workflowId, Vector.empty, _))
  }

  override def deleteWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: Vector[String] = Vector.empty
  ): Long = {
    val pattern = likeEscaped(keyPrefix) + "%"
    runTransaction {
      for {
        _ <- sql"""DELETE FROM workflow_events
                   WHERE workflow_id = $workflowId AND workflow_instance_scope = $scope AND workflow_instance_key LIKE $pattern ESCAPE '\'""".update.run
        deleted <- sql"""DELETE FROM workflow_instances
                         WHERE workflow_id = $workflowId AND workflow_instance_scope = $scope AND workflow_instance_key LIKE $pattern ESCAPE '\'""".update.run
      } yield deleted.toLong
    }
  }

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

    def readTerminalOrThrowMissing(): Option[(String, Option[String])] = {
      val row = runTransaction {
        sql"""SELECT terminal_state, terminal_outcome FROM workflow_instances
              WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".query[
            (Option[String], Option[String])
          ].option
      }
      row match {
        case None => throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
        case Some((state, outcome)) => state.map(s => (s, outcome))
      }
    }

    val deadlineNanos = System.nanoTime() + timeout.toNanos
    var terminal: Option[(String, Option[String])] = None
    var expired = false
    while (terminal.isEmpty && !expired) {
      terminal = readTerminalOrThrowMissing()
      if (terminal.isEmpty) {
        if (System.nanoTime() >= deadlineNanos) expired = true
        else Thread.sleep(AwaitResultPollIntervalMillis)
      }
    }

    terminal match {
      case Some((state, outcome)) => handleTerminalRead(state, outcome, outCacheable, completionCodec, instanceId)
      case None =>
        throw new java.util.concurrent.TimeoutException(
          s"Workflow instance did not reach a terminal state within $timeout: $instanceId"
        )
    }
  }

  override def getWorkflowInstanceInfo[In, Out](
      instance: WorkflowInstance[In, Out]
  ): WorkflowInstance.Info = {
    val instanceId = instance.id
    val row = runTransaction {
      (InfoSelect ++ fr"WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey} AND workflow_instance_scope = ${instanceId.scope}")
        .query[InfoRow]
        .option
    }
    row match {
      case Some(r) => toInfo(instanceId.workflowId, instanceId.scope, r)
      case None    => throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
    }
  }

  override def getChildWorkflowInstances(parentId: WorkflowInstanceId): Vector[WorkflowInstance.Info] = {
    val rows = runTransaction {
      sql"""SELECT workflow_id, workflow_instance_scope, workflow_instance_key, workflow_version_at_creation, generation,
                   parent_workflow_id, parent_instance_key, parent_instance_scope, created_at, last_run_at, times_executed, terminal_state
            FROM workflow_instances
            WHERE parent_workflow_id = ${parentId.workflowId}
              AND parent_instance_key = ${parentId.workflowInstanceKey}
              AND parent_instance_scope = ${parentId.scope}
            ORDER BY workflow_id, workflow_instance_key, workflow_instance_scope""".query[(WorkflowId, Vector[String], InfoRow)].to[Vector]
    }
    rows.map { case (wf, scope, r) => toInfo(wf, scope, r) }
  }

  // ---- Engine operations of the public WorkflowRuntime SPI. Each takes this
  // ---- run's PostgresCurrentExecution handle carrying the run's fencing identity.

  override def lookupStep(run: CurrentExecution, stepId: StepId, stepVersion: Long): Option[StoredStep] =
    readStepRow(run.workflowId, run.instanceKey, run.instanceScope, stepId.key, stepId.scope, stepVersion)

  override def writeStepState(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      state: StoredStep.State,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[Instant]
  ): Unit =
    fenced(run) {
      for {
        _ <- upsertStepStateIO(
          run, stepId, stepVersion, stepKind, state, inputFingerprints, payload, expiresAt
        )
        _ <- deleteTimerSubscriptionsIO(run, stepId, stepVersion)
      } yield ()
    }

  override def deleteStep(run: CurrentExecution, stepId: StepId, stepVersion: Long): Unit =
    fenced(run) {
      for {
        _ <- sql"""DELETE FROM workflow_steps
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope} AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run
        _ <- deleteTimerSubscriptionsIO(run, stepId, stepVersion)
      } yield ()
    }

  override def retireAwaitSite(run: CurrentExecution, stepId: StepId, stepVersion: Long): Unit =
    fenced(run) {
      for {
        _ <- sql"""DELETE FROM workflow_steps
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope} AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run
        _ <- deleteSiteSubscriptionsIO(run, stepId, stepVersion)
      } yield ()
    }

  override def suspendStepRetry(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      retryPayload: String,
      deadline: Instant,
      expiresAt: Option[Instant]
  ): Unit =
    fenced(run) {
      for {
        _ <- upsertStepStateIO(
          run,
          stepId,
          stepVersion,
          stepKind,
          StoredStep.State.Started,
          inputFingerprints,
          retryPayload,
          expiresAt
        )
        _ <- insertRetrySubscriptionIO(run, stepId, stepVersion, deadline)
      } yield ()
    }

  override def fireStepRetryIfDue(run: CurrentExecution, stepId: StepId, stepVersion: Long): Boolean =
    fenced(run) {
      val now = theClock.instant()
      for {
        subs <- sql"""SELECT timer_id, deadline FROM workflow_timer_subscriptions
                      WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                        AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion AND subscriber_key = $RetrySubscriberKey
                      ORDER BY deadline, timer_id
                      FOR UPDATE""".query[(java.util.UUID, Instant)].to[Vector]
        _ <- subs.collect { case (subId, deadline) if !deadline.isAfter(now) => subId }.traverse_ { subId =>
          for {
            exists <- sql"""SELECT 1 FROM workflow_events
                            WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                              AND event_kind = 'TimerFired' AND event_key = ${subId.toString}""".query[Int].option
            _ <- if (exists.isEmpty) appendEvent(run.workflowId, run.instanceKey, run.instanceScope, "TimerFired", subId.toString, "")
                 else ().pure[ConnectionIO]
          } yield ()
        }
        fired <- if (subs.isEmpty) false.pure[ConnectionIO]
                 else {
                   val idStrings = subs.map(_._1.toString)
                   fr"""SELECT 1 FROM workflow_events
                         WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                           AND event_kind = 'TimerFired' AND event_key = ANY($idStrings)
                         LIMIT 1""".query[Int].option.map(_.isDefined)
                 }
      } yield fired
    }

  override def readAwaitSignalCandidates(run: CurrentExecution, signalKey: SignalKey): Vector[SubscriberMatch] =
    runTransaction {
      for {
        cursor <- sql"""SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                        WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope} AND signal_key = $signalKey""".query[Long].unique
        direct <- sql"""SELECT sequence_id, payload, created_at FROM workflow_events
                        WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                          AND event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor
                        ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
        inherited <- readInheritedCandidatesIO(run, signalKey, cursor)
      } yield (direct ++ inherited).sortBy(_._1).map { case (seq, payload, createdAt) =>
        SubscriberMatch("", seq, payload, createdAt)
      }
    }

  override def readAwaitUpdateCandidates(run: CurrentExecution, updateKey: String): Vector[UpdateCandidate] =
    runTransaction {
      sql"""SELECT created_at, idempotency_key, encoded_input FROM workflow_updates
            WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
              AND update_key = $updateKey AND handled_at IS NULL
            ORDER BY created_at""".query[(java.time.Instant, String, String)].to[Vector]
    }.map { case (createdAt, idem, encodedInput) => UpdateCandidate(createdAt, idem, encodedInput) }

  override def resolveAwaitUpdate(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      updateKey: String,
      candidate: UpdateCandidate,
      encodedResponse: String,
      encodedOutput: String,
      expiresAt: Option[Instant]
  ): Boolean =
    fenced(run) {
      for {
        updated <- handleUpdateRecordIO(run, updateKey, candidate, encodedResponse)
        won = updated == 1
        _ <- if (won) upsertStepStateIO(
               run,
               stepId,
               stepVersion,
               stepKind,
               StoredStep.State.Succeeded,
               inputFingerprints,
               encodedOutput,
               expiresAt
             )
             else ().pure[ConnectionIO]
        _ <- if (won) deleteUpdateSubscriptionsIO(run, stepId, stepVersion) else ().pure[ConnectionIO]
      } yield won
    }

  override def suspendAwaitUpdate(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      inputFingerprints: String,
      updateKey: String,
      expiresAt: Option[Instant]
  )(decide: Vector[UpdateCandidate] => Option[AwaitUpdateDecision]): Option[AwaitUpdateDecision] =
    fenced(run) {
      for {
        _ <- upsertUpdateSubscriptionIO(run, stepId, stepVersion, updateKey)
        candidates <- readUpdateCandidatesIO(run, updateKey)
        decision = decide(candidates)
        resolved <- decision match {
          case Some(d) =>
            for {
              updated <- handleUpdateRecordIO(run, updateKey, d.candidate, d.encodedResponse)
              won = updated == 1
              _ <- if (won) upsertStepStateIO(
                     run,
                     stepId,
                     stepVersion,
                     stepKind,
                     StoredStep.State.Succeeded,
                     inputFingerprints,
                     d.encodedOutput,
                     expiresAt
                   )
                   else ().pure[ConnectionIO]
              _ <- if (won) deleteUpdateSubscriptionsIO(run, stepId, stepVersion) else ().pure[ConnectionIO]
            } yield if (won) Some(d) else None
          case None => Option.empty[AwaitUpdateDecision].pure[ConnectionIO]
        }
      } yield resolved
    }

  override def invalidateTimer(run: CurrentExecution, stepId: StepId, stepVersion: Long, deadline: Instant): Unit =
    fenced(run) {
      for {
        _ <- sql"""DELETE FROM workflow_steps
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                     AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run
        _ <- deleteTimerSubscriptionsIO(run, stepId, stepVersion)
        _ <- sql"""INSERT INTO workflow_timer_subscriptions
                    (timer_id, workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, deadline)
                  VALUES (gen_random_uuid(), ${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, '', $deadline)
                  ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, step_scope_within_instance) DO NOTHING""".update.run
      } yield ()
    }

  override def evaluateWait(
      run: CurrentExecution,
      site: WaitSite,
      subscribers: Vector[Subscriber]
  )(decide: Vector[SubscriberMatch] => Option[WaitResolution]): Option[String] = {
    fenced(run)(fireDueTimerSubscriptionsIO(run, site.stepId, site.stepVersion))
    fenced(run) {
      for {
        _ <- subscribers.traverse_(registerInterestIO(run, site, _))
        perSubscriber <- subscribers.traverse(sub => readInterestCandidatesIO(run, site, sub).map(sub -> _))
        candidates = perSubscriber.flatMap(_._2).sortBy(_.sequenceId)
        resolution = decide(candidates)
        result <- resolution match {
          case Some(r) =>
            for {
              _ <- upsertStepStateIO(
                     run,
                     site.stepId,
                     site.stepVersion,
                     site.stepKind,
                     StoredStep.State.Succeeded,
                     site.inputFingerprints,
                     r.payload,
                     site.expiresAt
                   )
              _ <- r.advanceSignalCursor.traverse_ { case (k, seq) => advanceCursorIO(run, k, seq) }
              _ <- deleteSiteSubscriptionsIO(run, site.stepId, site.stepVersion)
            } yield Some(r.payload)
          case None =>
            val advances: Map[SignalKey, Long] =
              perSubscriber.foldLeft(Map.empty[SignalKey, Long]) {
                case (acc, (Subscriber.Signal(_, k), cs)) if cs.nonEmpty =>
                  val mx = cs.iterator.map(_.sequenceId).max
                  acc.updated(k, acc.get(k).fold(mx)(math.max(_, mx)))
                case (acc, _) => acc
              }
            advances.toVector
              .traverse_ { case (k, seq) => advanceCursorIO(run, k, seq) }
              .as(None)
        }
      } yield result
    }
  }

  override def deleteSubscriptionsUnderScopePaths(run: CurrentExecution, scopePaths: Seq[Vector[String]]): Unit =
    fenced(run)(deleteSubscriptionsUnderScopePathsIO(run, scopePaths))

  override def readRegionState(
      run: CurrentExecution,
      regionId: String,
      parentScopePath: Vector[String]
  ): Option[(String, Long)] =
    runTransaction {
      sql"""SELECT state_payload FROM workflow_steps
            WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
              AND step_id = $regionId AND step_scope_within_instance = $parentScopePath
              AND step_kind = 'RestartableRegion'""".query[String].option.map(_.map(parseRegionPayload))
    }

  override def upsertRegion(
      run: CurrentExecution,
      regionId: String,
      parentScopePath: Vector[String],
      restartCount: Long,
      serializedState: String
  ): Unit =
    fenced(run) {
      val base = regionInteriorBase(regionId, parentScopePath, restartCount - 1)
      val now = theClock.instant()
      for {
        _ <- deleteRegionNestedStepsIO(run, base)
        _ <- deleteRegionNestedSubscriptionsIO(run, base)
        // Only a restart discards a prior interior; the first creation
        // (restartCount 0) closes nothing and records no closure.
        _ <- if (restartCount > 0)
          insertChildCloseRowIO(
            run.workflowId,
            run.instanceKey,
            run.instanceScope,
            run.instanceScope ++ Vector(run.workflowId, s"${run.instanceKey}@${run.generation}") ++ base
          )
        else ().pure[ConnectionIO]
        _ <- sql"""INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
              VALUES (${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, $regionId, $parentScopePath, 0, 'RestartableRegion', 'Started', ${regionPayload(restartCount, serializedState)}, '', NULL, $now, $now)
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, step_scope_within_instance)
              DO UPDATE SET state_payload = EXCLUDED.state_payload, updated_at = EXCLUDED.updated_at""".update.run
      } yield ()
    }

  override def heartbeat(run: CurrentExecution): Unit = renewLease(run)

  override def isCancellationRequested(instanceId: WorkflowInstanceId): Boolean = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    runTransaction {
      sql"""SELECT cancel_requested_at FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".query[
          Option[java.time.Instant]
        ].unique
    }.isDefined
  }

  private def renewLease(run: CurrentExecution): Unit = {
    val now = theClock.instant()
    val expires = now.plus(java.time.Duration.ofNanos(leaseDuration.toNanos))
    val updated = runTransaction {
      sql"""UPDATE workflow_instances
            SET lease_expires_at = $expires
            WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
              AND lease_owner = ${run.workerId} AND fencing_token = ${run.fencingToken}
              AND terminal_state IS NULL""".update.run
    }
    if (updated != 1) throw LeaseLostException(run.instanceId)
  }

  /** Runs `write` inside one transaction, guarded by an exclusive lock on the
    * instance row and a fencing check; throws [[LeaseLostException]] if the
    * lease no longer belongs to this run, affecting no rows. Callers that do
    * not need the result simply discard it.
    */
  private def fenced[A](run: CurrentExecution)(write: ConnectionIO[A]): A = {
    val res: Option[A] = runTransaction {
      for {
        _ <- sql"""SELECT 1 FROM workflow_instances
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   FOR UPDATE""".query[Int].unique
        fenceOk <- sql"""SELECT 1 FROM workflow_instances
                         WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                           AND lease_owner = ${run.workerId} AND fencing_token = ${run.fencingToken}""".query[Int].option
        r <- if (fenceOk.isDefined) write.map(Some(_)) else None.pure[ConnectionIO]
      } yield r
    }
    res match {
      case Some(a) => a
      case None    => throw LeaseLostException(run.instanceId)
    }
  }

  private val RetrySubscriberKey: String = "__retry__"

  private def upsertStepStateIO(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      stepKind: String,
      state: StoredStep.State,
      inputFingerprints: String,
      payload: String,
      expiresAt: Option[java.time.Instant]
  ): ConnectionIO[Unit] = {
    val now = theClock.instant()
    val kind = state.toString
    sql"""INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
          VALUES (${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $stepKind, $kind, $payload, $inputFingerprints, $expiresAt, $now, $now)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, step_scope_within_instance) DO UPDATE
          SET state_kind = $kind, state_payload = EXCLUDED.state_payload, input_fingerprints = EXCLUDED.input_fingerprints, expires_at = EXCLUDED.expires_at, updated_at = $now""".update.run.map(
      _ => ()
    )
  }

  private def insertRetrySubscriptionIO(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      deadline: java.time.Instant
  ): ConnectionIO[Unit] =
    for {
      _ <- sql"""DELETE FROM workflow_timer_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion AND subscriber_key = $RetrySubscriberKey""".update.run
      _ <- sql"""INSERT INTO workflow_timer_subscriptions
                  (timer_id, workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, deadline)
                VALUES (gen_random_uuid(), ${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $RetrySubscriberKey, $deadline)
                ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, step_scope_within_instance) DO NOTHING""".update.run
    } yield ()

  private def readUpdateCandidatesIO(run: CurrentExecution, updateKey: String): ConnectionIO[Vector[UpdateCandidate]] =
    sql"""SELECT created_at, idempotency_key, encoded_input FROM workflow_updates
          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
            AND update_key = $updateKey AND handled_at IS NULL
          ORDER BY created_at""".query[(java.time.Instant, String, String)].to[Vector]
      .map(_.map { case (createdAt, idem, encodedInput) => UpdateCandidate(createdAt, idem, encodedInput) })

  private def handleUpdateRecordIO(
      run: CurrentExecution,
      updateKey: String,
      candidate: UpdateCandidate,
      encodedResponse: String
  ): ConnectionIO[Int] = {
    val now = theClock.instant()
    sql"""UPDATE workflow_updates
          SET result = $encodedResponse, handled_at = $now, updated_at = $now
          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
            AND update_key = $updateKey AND idempotency_key = ${candidate.idempotencyKey}
            AND created_at = ${candidate.createdAt} AND handled_at IS NULL""".update.run
  }

  private def upsertUpdateSubscriptionIO(
      run: CurrentExecution,
      stepId: StepId,
      stepVersion: Long,
      updateKey: String
  ): ConnectionIO[Unit] =
    sql"""INSERT INTO workflow_update_subscriptions
            (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, update_key)
          VALUES (${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, '', $updateKey)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, update_key, step_scope_within_instance)
            DO NOTHING""".update.run.map(_ => ())

  private def deleteUpdateSubscriptionsIO(run: CurrentExecution, stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
    sql"""DELETE FROM workflow_update_subscriptions
          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
            AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run.map(
      _ => ()
    )

  private def fireDueTimerSubscriptionsIO(run: CurrentExecution, stepId: StepId, stepVersion: Long): ConnectionIO[Unit] = {
    val now = theClock.instant()
    for {
      due <- sql"""SELECT timer_id FROM workflow_timer_subscriptions
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                     AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion
                     AND deadline <= $now
                   ORDER BY deadline, timer_id
                   FOR UPDATE""".query[java.util.UUID].to[Vector]
      _ <- due.traverse_ { subId =>
        for {
          exists <- sql"""SELECT 1 FROM workflow_events
                          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                            AND event_kind = 'TimerFired' AND event_key = ${subId.toString}""".query[Int].option
          _ <- if (exists.isEmpty) appendEvent(run.workflowId, run.instanceKey, run.instanceScope, "TimerFired", subId.toString, "")
               else ().pure[ConnectionIO]
        } yield ()
      }
    } yield ()
  }

  private def registerInterestIO(run: CurrentExecution, site: WaitSite, interest: Subscriber): ConnectionIO[Unit] =
    interest match {
      case Subscriber.Signal(subscriberKey, signalKey) =>
        sql"""INSERT INTO workflow_signal_subscriptions (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, signal_key)
              VALUES (${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${site.stepId.key}, ${site.stepId.scope}, ${site.stepVersion}, $subscriberKey, $signalKey)
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, signal_key) DO NOTHING""".update.run.map(_ => ())
      case Subscriber.Timer(subscriberKey, deadline) =>
        sql"""INSERT INTO workflow_timer_subscriptions
                (timer_id, workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, deadline)
              VALUES (gen_random_uuid(), ${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${site.stepId.key}, ${site.stepId.scope}, ${site.stepVersion}, $subscriberKey, $deadline)
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, step_scope_within_instance) DO NOTHING""".update.run.map(_ => ())
      case Subscriber.Completion(subscriberKey, completedWorkflowId, completedWorkflowInstanceKey, completedScope) =>
        sql"""INSERT INTO workflow_completion_subscriptions
                (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, completed_workflow_id, completed_workflow_instance_key, completed_workflow_instance_scope)
              VALUES (${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, ${site.stepId.key}, ${site.stepId.scope}, ${site.stepVersion}, $subscriberKey, $completedWorkflowId, $completedWorkflowInstanceKey, $completedScope)
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, completed_workflow_id, completed_workflow_instance_key, completed_workflow_instance_scope) DO NOTHING""".update.run.map(_ => ())
    }

  private def readInterestCandidatesIO(run: CurrentExecution, site: WaitSite, interest: Subscriber): ConnectionIO[Vector[SubscriberMatch]] =
    interest match {
      case Subscriber.Signal(subscriberKey, signalKey) =>
        for {
          cursor <- sql"""SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
                          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope} AND signal_key = $signalKey""".query[Long].unique
          direct <- sql"""SELECT sequence_id, payload, created_at FROM workflow_events
                          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                            AND event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor
                          ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
          inherited <- readInheritedCandidatesIO(run, signalKey, cursor)
        } yield (direct ++ inherited).sortBy(_._1).map { case (seq, payload, createdAt) =>
          SubscriberMatch(subscriberKey, seq, payload, createdAt)
        }
      case Subscriber.Timer(subscriberKey, _) =>
        for {
          subId <- sql"""SELECT timer_id FROM workflow_timer_subscriptions
                         WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                           AND step_id = ${site.stepId.key} AND step_scope_within_instance = ${site.stepId.scope} AND step_version = ${site.stepVersion} AND subscriber_key = $subscriberKey""".query[java.util.UUID].option
          events <- subId match {
            case None => Vector.empty[SubscriberMatch].pure[ConnectionIO]
            case Some(id) =>
              sql"""SELECT sequence_id, created_at FROM workflow_events
                    WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                      AND event_kind = 'TimerFired' AND event_key = ${id.toString}
                    ORDER BY sequence_id""".query[(Long, java.time.Instant)].to[Vector]
                .map(_.map { case (seq, createdAt) => SubscriberMatch(subscriberKey, seq, "", createdAt) })
          }
        } yield events
      case Subscriber.Completion(subscriberKey, completedWorkflowId, completedWorkflowInstanceKey, completedScope) =>
        sql"""SELECT sequence_id, payload, created_at FROM workflow_events
              WHERE workflow_id = $completedWorkflowId AND workflow_instance_key = $completedWorkflowInstanceKey AND workflow_instance_scope = $completedScope
                AND event_kind = 'WorkflowCompleted' AND event_key = ''
              ORDER BY sequence_id""".query[(Long, String, java.time.Instant)].to[Vector]
          .map(_.map { case (seq, payload, createdAt) => SubscriberMatch(subscriberKey, seq, payload, createdAt) })
    }

  private def deleteSiteSubscriptionsIO(run: CurrentExecution, stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
    for {
      _ <- sql"""DELETE FROM workflow_signal_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run
      _ <- sql"""DELETE FROM workflow_timer_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run
      _ <- sql"""DELETE FROM workflow_completion_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run
    } yield ()

  /** Deletes the subscription rows whose `step_scope_within_instance` equals one of
    * `scopePaths` or lies beneath one (a longer array with one of `scopePaths` as
    * a prefix), across all four subscription tables. `scopePaths` are always
    * non-empty, so the array-prefix equality is a valid slice predicate.
    */
  private def deleteSubscriptionsUnderScopePathsIO(run: CurrentExecution, scopePaths: Seq[Vector[String]]): ConnectionIO[Unit] =
    scopePaths.traverse_ { path =>
      require(path.nonEmpty, "a step scope path must never be empty; an empty prefix would match every row")
      for {
        _ <- sql"""DELETE FROM workflow_signal_subscriptions
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                     AND step_scope_within_instance[1:${path.length}] = $path""".update.run
        _ <- sql"""DELETE FROM workflow_timer_subscriptions
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                     AND step_scope_within_instance[1:${path.length}] = $path""".update.run
        _ <- sql"""DELETE FROM workflow_completion_subscriptions
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                     AND step_scope_within_instance[1:${path.length}] = $path""".update.run
        _ <- sql"""DELETE FROM workflow_update_subscriptions
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                     AND step_scope_within_instance[1:${path.length}] = $path""".update.run
      } yield ()
    }

  private def deleteTimerSubscriptionsIO(run: CurrentExecution, stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
    sql"""DELETE FROM workflow_timer_subscriptions
          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
            AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion""".update.run.map(_ => ())

  private def advanceCursorIO(run: CurrentExecution, signalKey: SignalKey, sequenceId: Long): ConnectionIO[Unit] =
    sql"""INSERT INTO signal_cursor (workflow_id, workflow_instance_key, workflow_instance_scope, signal_key, sequence_id)
          VALUES (${run.workflowId}, ${run.instanceKey}, ${run.instanceScope}, $signalKey, $sequenceId)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, signal_key) DO UPDATE
          SET sequence_id = GREATEST(signal_cursor.sequence_id, EXCLUDED.sequence_id)""".update.run.map(_ => ())

  /** Walks the active-parent chain from this instance upward and returns the
    * (workflow_id, workflow_instance_key, scope) identities of the ancestor sources whose
    * directly addressed events this instance may inherit for `signalKey`. An
    * ancestor is a source only when every parent-child edge on the path
    * currently permits the key; the policy governing the edge from an instance
    * to its own parent is that instance's stored inheritance selector. A
    * cleared or missing parent pointer (detachment) terminates the walk, so
    * abandoned children inherit nothing. A parent also terminates the walk when
    * it is terminal or has recorded a pending `ChildClose` matching the child's
    * scope: detachment is logically immediate with the closure event even
    * though the pointer is cleared only at asynchronous delivery, so no
    * transitive ancestor event can leak through a closed parent in the window.
    */
  private def inheritedSourceScopesIO(
      run: CurrentExecution,
      signalKey: SignalKey
  ): ConnectionIO[Vector[(WorkflowId, WorkflowInstanceKey, Vector[String])]] = {
    def load(
        w: WorkflowId,
        kk: WorkflowInstanceKey,
        ss: Vector[String]
    ): ConnectionIO[Option[(Option[String], Option[String], Option[String], Option[Vector[String]])]] =
      sql"""SELECT inherit_signals, parent_workflow_id, parent_instance_key, parent_instance_scope
            FROM workflow_instances WHERE workflow_id = $w AND workflow_instance_key = $kk AND workflow_instance_scope = $ss""".query[
            (Option[String], Option[String], Option[String], Option[Vector[String]])
          ].option
    /** Whether `(f, k, s)` is a usable inheritance source for the child scope
      * `ss`: present, not terminal, and not behind a pending `ChildClose` that
      * targets the child.
      */
    def usableSource(
        f: WorkflowId,
        k: WorkflowInstanceKey,
        s: Vector[String],
        ss: Vector[String]
    ): ConnectionIO[Boolean] =
      sql"""SELECT 1 FROM workflow_instances p
            WHERE p.workflow_id = $f AND p.workflow_instance_key = $k AND p.workflow_instance_scope = $s
              AND p.terminal_state IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM workflow_steps r
                WHERE r.workflow_id = $f AND r.workflow_instance_key = $k AND r.workflow_instance_scope = $s
                  AND r.step_kind = 'ChildClose'
                  AND ($ss::text[])[1:COALESCE(array_length(r.step_scope_within_instance,1),0)] = r.step_scope_within_instance)
            LIMIT 1""".query[Int].option.map(_.isDefined)
    def loop(
        node: (Option[String], Option[String], Option[String], Option[Vector[String]]),
        nodeScope: Vector[String]
    ): ConnectionIO[Vector[(WorkflowId, WorkflowInstanceKey, Vector[String])]] = {
      val (policy, pwf, pkey, pscope) = node
      (policy, pwf, pkey, pscope) match {
        case (Some(p), Some(f), Some(k), Some(s)) if inheritancePermits(decodeSignalInheritance(Some(p)), signalKey) =>
          for {
            usable <- usableSource(f, k, s, nodeScope)
            sources <-
              if (!usable) Vector.empty[(WorkflowId, WorkflowInstanceKey, Vector[String])].pure[ConnectionIO]
              else
                for {
                  parent <- load(f, k, s)
                  rest <- parent match {
                    case Some(pn) => loop(pn, s)
                    case None     => Vector.empty[(WorkflowId, WorkflowInstanceKey, Vector[String])].pure[ConnectionIO]
                  }
                } yield (f, k, s) +: rest
          } yield sources
        case _ => Vector.empty[(WorkflowId, WorkflowInstanceKey, Vector[String])].pure[ConnectionIO]
      }
    }
    for {
      seed <- load(run.workflowId, run.instanceKey, run.instanceScope)
      sources <- seed match {
        case Some(sn) => loop(sn, run.instanceScope)
        case None     => Vector.empty[(WorkflowId, WorkflowInstanceKey, Vector[String])].pure[ConnectionIO]
      }
    } yield sources
  }

  /** The inherited `Signal` candidates for `signalKey` on this instance: events
    * stored on any currently eligible ancestor, after the instance's own cursor
    * and (unless `inheritPastEvents`) after the inherited-events start
    * sequence id captured at creation. With `inheritPastEvents` the retained
    * older events remain visible while they are ahead of the instance's cursor.
    */
  private def readInheritedCandidatesIO(
      run: CurrentExecution,
      signalKey: SignalKey,
      cursor: Long
  ): ConnectionIO[Vector[(Long, String, java.time.Instant)]] =
    for {
      cfg <- sql"""SELECT inherit_past_events, inherited_events_start_sequence_id FROM workflow_instances
                   WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}""".query[(Boolean, Option[Long])].option
      sources <- inheritedSourceScopesIO(run, signalKey)
      events <- if (sources.isEmpty) Vector.empty[(Long, String, java.time.Instant)].pure[ConnectionIO]
                else {
                  val sourceCond = sources
                    .map { case (w, kk, ss) => fr"(workflow_id = $w AND workflow_instance_key = $kk AND workflow_instance_scope = $ss)" }
                    .reduce(_ ++ fr" OR " ++ _)
                  val (pastEvents, startSeq) = cfg.getOrElse((false, None))
                  val window =
                    if (pastEvents) Fragment.empty
                    else startSeq.map(seq => fr"AND sequence_id > $seq").getOrElse(Fragment.empty)
                  (fr"""SELECT sequence_id, payload, created_at FROM workflow_events
                        WHERE event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor AND (""" ++
                    sourceCond ++ fr")" ++ window ++ fr" ORDER BY sequence_id").query[(Long, String, java.time.Instant)].to[Vector]
                }
    } yield events

  /** The step-scope subtree prefix of a region's interior at a given
    * `restartCount`: the parent scope path followed by the raw `regionId@count`
    * segment. Nested Step/Await rows and children of the looping live under this
    * prefix; the region's own row lives at the parent scope path (one level up).
    */
  private def regionInteriorBase(regionId: String, parentScopePath: Vector[String], count: Long): Vector[String] =
    parentScopePath :+ s"$regionId@$count"

  /** The wrapped payload of a region row: its committed `restartCount` and the
    * encoded user state, stored in the step row's `state_payload`.
    */
  private case class RegionPayload(count: Long, state: String) derives upickle.default.ReadWriter

  private def regionPayload(count: Long, state: String): String =
    upickle.default.write(RegionPayload(count, state))

  private def parseRegionPayload(p: String): (String, Long) = {
    val r = upickle.default.read[RegionPayload](p)
    (r.state, r.count)
  }

  /** Deletes the nested Step rows under the region's interior subtree: rows whose
    * `step_scope_within_instance` is `base` or lies beneath it. `base` is the
    * region interior base (always non-empty), so the array-prefix equality is a
    * valid slice predicate. Pending `ChildClose` rows are runtime bookkeeping
    * and never belong to a discarded interior, so they are excluded
    * defensively.
    */
  private def deleteRegionNestedStepsIO(run: CurrentExecution, base: Vector[String]): ConnectionIO[Unit] =
    sql"""DELETE FROM workflow_steps
          WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
            AND step_kind <> 'ChildClose'
            AND step_scope_within_instance[1:${base.length}] = $base""".update.run.map(_ => ())

  /** Deletes the nested subscription rows under the region's interior subtree:
    * rows whose `step_scope_within_instance` is `base` or lies beneath it, in
    * the signal, timer, and completion subscription tables. `base` is the region
    * interior base (always non-empty), so the array-prefix equality is a valid
    * slice predicate.
    */
  private def deleteRegionNestedSubscriptionsIO(run: CurrentExecution, base: Vector[String]): ConnectionIO[Unit] =
    for {
      _ <- sql"""DELETE FROM workflow_signal_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_scope_within_instance[1:${base.length}] = $base""".update.run
      _ <- sql"""DELETE FROM workflow_timer_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_scope_within_instance[1:${base.length}] = $base""".update.run
      _ <- sql"""DELETE FROM workflow_completion_subscriptions
                 WHERE workflow_id = ${run.workflowId} AND workflow_instance_key = ${run.instanceKey} AND workflow_instance_scope = ${run.instanceScope}
                   AND step_scope_within_instance[1:${base.length}] = $base""".update.run
    } yield ()

  /** The per-run execution handle of this runtime: the fencing identity of one
    * run (worker, fencing token, generation) plus the instance identity. The
    * runtime creates it when a run's lease is acquired; workflow code only
    * passes it back to this runtime's engine operations, which are direct
    * members of the runtime taking the handle as their first parameter.
    */
  final class PostgresCurrentExecution(
      val workerId: String,
      val fencingToken: Long,
      val workflowId: WorkflowId,
      val instanceKey: WorkflowInstanceKey,
      val instanceScope: Vector[String],
      val generation: Long
  ) {

    private[PostgresWorkflowRuntime] val instanceId = WorkflowInstanceId(workflowId, instanceKey, instanceScope)
  }

  override def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit = {
    val now = theClock.instant()
    val scheduledAt = now.plus(java.time.Duration.ofNanos(delay.toNanos))
    runTransaction {
      sql"""
        INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
        VALUES (${instanceId.workflowId}, ${instanceId.workflowInstanceKey}, ${instanceId.scope}, $now, $scheduledAt, 0)
        ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
      """.update.run
    }
    ()
  }

  /** The instance's terminal state, or `None` if not yet terminal. Used by the
    * job runner to classify a run's outcome by durable state rather than by
    * exception type.
    */
  private[atomicflow] def readTerminalState(instanceId: WorkflowInstanceId): Option[String] =
    runTransaction {
      sql"""SELECT terminal_state FROM workflow_instances
            WHERE workflow_id = ${instanceId.workflowId} 
              AND workflow_instance_key = ${instanceId.workflowInstanceKey}
               AND workflow_instance_scope = ${instanceId.scope}
      """.query[Option[String]].unique
    }

  /** Whether the instance's lease is still owned by `worker` at `token` — i.e.
    * this exact lease incarnation still holds it. */
  private[atomicflow] def isLeaseStillOwned(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Boolean =
    runTransaction {
      sql"""SELECT 1 FROM workflow_instances
            WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey} AND workflow_instance_scope = ${instanceId.scope}
              AND lease_owner = $worker AND fencing_token = $token""".query[Int].option
    }.isDefined

  override def startJobRunner(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings = JobRunnerSettings.default
  ): JobRunner =
    new PostgresJobRunner(this, definitions, settings, startLoop = true)

  /** Package-private test hook: like [[startJobRunner]] but builds a runner whose
    * background driver loop is NOT started, so a test can drive claim cycles
    * deterministically via `runDriverCycle` without racing the auto-loop.
    */
  private[atomicflow] def startJobRunnerForTests(
      definitions: Seq[Workflow[?, ?]],
      settings: JobRunnerSettings
  ): PostgresJobRunner =
    new PostgresJobRunner(this, definitions, settings, startLoop = false)
}
