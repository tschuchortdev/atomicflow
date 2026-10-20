package atomicflow.impl.db

import atomicflow.*
import atomicflow.internal.Framing
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

import java.time.Clock
import java.time.Instant
import java.util.UUID
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object PostgresWorkflowRuntime {
  def apply(dataSource: DataSource)(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(dataSource, Clock.systemUTC())

  def apply(dataSource: DataSource, clock: Clock)(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(dataSource, clock)

  def apply(
      dataSource: DataSource,
      clock: Clock,
      leaseDuration: FiniteDuration = 5.minutes,
      leaseAcquireTimeout: FiniteDuration = 30.seconds,
      durableRetryThreshold: FiniteDuration = 30.seconds
  )(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(dataSource, clock, leaseDuration, leaseAcquireTimeout, durableRetryThreshold)
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
    dataSource: DataSource,
    override val clock: Clock,
    leaseDuration: FiniteDuration = 5.minutes,
    leaseAcquireTimeoutSetting: FiniteDuration = 30.seconds,
    durableRetryThresholdSetting: FiniteDuration = 30.seconds
)(using executionContext: ExecutionContext)
    extends WorkflowRuntime
    with WorkflowRuntimeInternals {

  Flyway.configure().dataSource(dataSource).load().migrate()

  private val log = LoggerFactory.getLogger(getClass)

  private val transactor = Transactor.fromDataSource[IO](dataSource, executionContext)

  private val processUuid: String = java.util.UUID.randomUUID().toString

  /** Well-known advisory-lock key for the global event-append protocol. */
  private val EventAppendLockKey: Long = 844810493715605001L

  private def clearInterrupt(): Unit = Thread.interrupted()

  private[atomicflow] def runTransaction[A](transaction: ConnectionIO[A]): A = {
    clearInterrupt()
    transaction.transact(transactor).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)
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

  /** This runtime's per-run handle: the fencing identity of one run (worker,
    * fencing token, generation) plus the instance identity, created when the
    * run's lease is acquired. The alias targets the class projection so the
    * member type does not depend on a particular runtime instance.
    */
  override type Lease = PostgresWorkflowRuntime#PostgresCurrentExecution

  def durableRetryThreshold: FiniteDuration = durableRetryThresholdSetting

  def leaseAcquireTimeout: FiniteDuration = leaseAcquireTimeoutSetting

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

  /** The updated_at timestamp of the row with the highest step_version stored
    * for one step id, or `None` when the step has no stored row. The timestamp
    * marks the last durable write to the step's most recent row.
    */
  private def getStepLastUpdatedAtTime(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
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
    val newId = WorkflowInstanceId(newWorkflowId, newInstanceKey, ScopePath.empty)

    runTransaction {
      for {
        sourceInput <- sql"""SELECT input FROM workflow_instances
                             WHERE workflow_id = $sourceWf AND workflow_instance_key = $sourceKey AND workflow_instance_scope = $sourceScope
                             FOR UPDATE""".query[String].option
        _ <- sourceInput match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $sourceInstanceId")
          case _ => ().pure[ConnectionIO]
        }
        boundary <- getStepLastUpdatedAtTime(sourceWf, sourceKey, sourceScope, restartFromStep)
        _ <- boundary match {
          case None =>
            throw new InvalidRestartStepException(
              s"restartFromStep ${restartFromStep.key}@'${restartFromStep.scope.encoded}' does not identify an executed step of $sourceInstanceId"
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
        // Copied rows keep their persisted state. Rows carrying a retry
        // deadline are excluded: they are suspended on their site's retry
        // timer, and timer subscriptions do not travel with the copy, so the
        // destination would otherwise suspend on a deadline nothing would ever
        // wake it for. The destination simply re-executes such steps from
        // scratch.
        _ <- sql"""
          INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, retry_deadline, input_fingerprints, expires_at, created_at, updated_at)
          SELECT $newWorkflowId, $newInstanceKey, '{}', step_id, step_scope_within_instance, step_version, step_kind,
            state_kind, state_payload, retry_deadline, input_fingerprints, expires_at, created_at, updated_at
          FROM workflow_steps
          WHERE workflow_id = $sourceWf AND workflow_instance_key = $sourceKey AND workflow_instance_scope = $sourceScope
            AND updated_at < $boundary
            AND retry_deadline IS NULL
        """.update.run
        _ <- upsertWakeupQ(newId.workflowId, newInstanceKey, ScopePath.empty, clock.instant())
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
    val now = clock.instant()
    runTransaction {
      for {
        row <- sql"""
          SELECT terminal_state, generation FROM workflow_instances
          WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          FOR UPDATE
        """.query[(Option[String], Long)].option
        generation = row.map(_._2).getOrElse(0L)
        _ <- row match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $sourceInstanceId")
          case Some((Some(_), _)) =>
            throw new IllegalStateException(s"Cannot reset a terminal workflow instance: $sourceInstanceId")
          case Some((None, _)) => ().pure[ConnectionIO]
        }
        boundary <- getStepLastUpdatedAtTime(wf, key, scope, restartFromStep)
        _ <- boundary match {
          case None =>
            throw new InvalidRestartStepException(
              s"restartFromStep ${restartFromStep.key}@'${restartFromStep.scope.encoded}' does not identify an executed step of $sourceInstanceId"
            )
          case _ => ().pure[ConnectionIO]
        }
        _ <- deleteAllSubscriptionsQ(
               wf,
               key,
               scope,
               fr"""EXISTS (SELECT 1 FROM workflow_steps st
                            WHERE st.workflow_id = $wf AND st.workflow_instance_key = $key AND st.workflow_instance_scope = $scope
                              AND st.updated_at >= $boundary
                              AND st.step_id = s.step_id AND st.step_scope_within_instance = s.step_scope_within_instance AND st.step_version = s.step_version)"""
             )
        _ <- sql"""DELETE FROM workflow_steps
                   WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                     AND updated_at >= $boundary""".update.run
        _ <- sql"""DELETE FROM workflow_wakeups
                   WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
        // Reset closes exactly the children of the old generation (targeting
        // the pre-increment generation, same as continueAsNew): children
        // created before the reset boundary are closed; body replay recreates
        // what is needed. The successor generation's children carry the
        // incremented marker and never match this target.
        _ <- insertChildCloseRequestQ(wf, key, scope, scope.segments ++ Vector(wf, s"$key@$generation"))
        _ <- sql"""UPDATE workflow_instances SET generation = generation + 1
                   WHERE workflow_id = $wf AND workflow_instance_key = $key AND workflow_instance_scope = $scope""".update.run
        _ <- upsertWakeupQ(wf, key, scope, now)
      } yield ()
    }
    ()
  }

  /** The storage form of a [[SignalInheritance]] configuration: the set of
    * permitted prefixes in `inherited_signal_prefixes`. The set is the complete
    * configuration — an empty set inherits nothing, the single empty-string
    * prefix (a prefix of every key) inherits everything, any other set inherits
    * keys starting with one of its elements.
    */
  private def permittedSignalPrefixes(inheritance: SignalInheritance): Vector[String] = inheritance match {
    case SignalInheritance.none           => Vector.empty
    case SignalInheritance.all            => Vector("")
    case SignalInheritance.some(prefixes) => prefixes.toVector
  }

  /** Renders a stored or replay-specified signal-inheritance configuration
    * (permitted-prefix set, past-signals flag) as one human-readable line,
    * used in the warning about ignored changes. Decodes the storage encoding:
    * an empty set is `none`, the single empty-string prefix is `all`.
    */
  private def renderSignalInheritanceConfig(prefixes: Vector[String], inheritPastEvents: Boolean): String = {
    val selector =
      if (prefixes.isEmpty) "none"
      else if (prefixes.lengthIs == 1 && prefixes.head.isEmpty) "all"
      else s"some(${prefixes.mkString(", ")})"
    s"inheritSignals=$selector, inheritPastEvents=$inheritPastEvents"
  }

  /** A `startAsChild` replay never replaces a child's signal-inheritance
    * configuration: the policy is fixed at creation. When the replay specifies a
    * different `(permitted prefixes, inherit_past_events)` than the stored row,
    * the stored configuration wins, the specified change is ignored, and a WARN
    * records the divergence. An equal configuration is a silent no-op, so a
    * replay writes nothing and schedules no wakeups.
    */
  private def warnOnIgnoredSignalInheritanceChange(
      childInstanceId: WorkflowInstanceId,
      childWorkflowId: WorkflowId,
      childInstanceKey: WorkflowInstanceKey,
      childScope: ScopePath,
      specifiedPrefixes: Vector[String],
      specifiedInheritPastEvents: Boolean
  ): Unit = {
    val stored = runTransaction {
      sql"""
        SELECT inherited_signal_prefixes, inherit_past_events
        FROM workflow_instances
        WHERE workflow_id = $childWorkflowId AND workflow_instance_key = $childInstanceKey AND workflow_instance_scope = $childScope
      """
        .query[(Vector[String], Boolean)].option
    }
    stored.foreach { case (prefixes, past) =>
      if (prefixes.toSet != specifiedPrefixes.toSet || past != specifiedInheritPastEvents)
        log.warn(
          s"Replaying startAsChild for child $childInstanceId with a different signal-inheritance configuration " +
            s"(stored: ${renderSignalInheritanceConfig(prefixes, past)}; specified: ${renderSignalInheritanceConfig(specifiedPrefixes, specifiedInheritPastEvents)}); " +
            "the configuration fixed at creation is kept."
        )
    }
  }

  /** The candidate rows read from the signal-subscription table: one row per
    * pending `(subscriber instance, signal key)` subscription, joined with the
    * subscriber's instance row so each candidate carries the subscriber's own
    * inheritance configuration. These are the instances whose awaits may be
    * satisfied by signals inherited from ancestors. Columns (positionally):
    * `sub_wf, sub_key, sub_scope, signal_key, node_wf, node_key, node_scope,
    * node_prefixes, parent_wf, parent_key, parent_scope, start_seq, past`.
    * `signalKeyFilter` may narrow the subscriptions it reads (the
    * dispatcher restricts to one signal key); it is appended to the WHERE clause
    * and may reference the `s` and `i` aliases.
    */
  private def signalSubscriberCandidates(signalKeyFilter: Fragment): Fragment =
    fr"""
      SELECT s.workflow_id, s.workflow_instance_key, s.workflow_instance_scope, s.signal_key,
             i.workflow_id, i.workflow_instance_key, i.workflow_instance_scope,
             i.inherited_signal_prefixes,
             i.parent_workflow_id, i.parent_instance_key, i.parent_instance_scope,
             i.inherited_events_start_sequence_id, i.inherit_past_events
      FROM workflow_signal_subscriptions s
      JOIN workflow_instances i
        ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
       WHERE i.terminal_state IS NULL
    """ ++ signalKeyFilter

  /** The read path's single candidate row: the running instance itself together
    * with the awaited signal key, so an await can resolve from inherited signals
    * before it has suspended and written a subscription row (peek reads before
    * suspension).
    */
  private def runningInstanceCandidate(lease: Lease, signalKey: SignalKey): Fragment =
    fr"""
      SELECT i.workflow_id, i.workflow_instance_key, i.workflow_instance_scope, $signalKey,
             i.workflow_id, i.workflow_instance_key, i.workflow_instance_scope,
             i.inherited_signal_prefixes,
             i.parent_workflow_id, i.parent_instance_key, i.parent_instance_scope,
             i.inherited_events_start_sequence_id, i.inherit_past_events
      FROM workflow_instances i
      WHERE i.workflow_id = ${lease.workflowId} AND i.workflow_instance_key = ${lease.instanceKey} AND i.workflow_instance_scope = ${lease.instanceScope}
    """

  /** Query shared by signal reads, the event dispatcher, and
    * the full subscription scan. Given `candidates` (subscriber rows produced by
    * [[signalSubscriberCandidates]] or [[runningInstanceCandidate]]), it climbs
    * each subscriber's active-parent chain and yields one row per instance
    * whose signals the subscriber may inherit:
    * `sub_wf, sub_key, sub_scope, signal_key, source_wf, source_key, source_scope,
    * past, start_seq`. An edge from a node to its parent is usable iff the
    * node's stored permitted-prefix set contains a prefix of the key (the
    * empty-string prefix permits every key, the empty set permits nothing). The
    * parent must be non-terminal and not
    * covered by a pending `child_close_requests` target for the child's scope.
    * The subscriber's own `inherit_past_events` and
    * `inherited_events_start_sequence_id` are carried unchanged (they belong to
    * the subscriber, not to an edge). There is no depth cap: the walk is bounded
    * by the parent chain.
    */
  private def inheritedSignalSources(candidates: Fragment): Fragment =
    fr"""
      WITH RECURSIVE ancestry(
        sub_wf, sub_key, sub_scope, signal_key,
        node_wf, node_key, node_scope, node_prefixes,
        parent_wf, parent_key, parent_scope, start_seq, past, depth) AS (
      SELECT candidates.*, 0
      FROM (
    """ ++ candidates ++
    fr"""
      ) candidates
        UNION ALL
        SELECT a.sub_wf, a.sub_key, a.sub_scope, a.signal_key,
               p.workflow_id, p.workflow_instance_key, p.workflow_instance_scope,
               p.inherited_signal_prefixes,
               p.parent_workflow_id, p.parent_instance_key, p.parent_instance_scope,
               a.start_seq, a.past, a.depth + 1
        FROM ancestry a
        JOIN workflow_instances p
          ON p.workflow_id = a.parent_wf AND p.workflow_instance_key = a.parent_key AND p.workflow_instance_scope = a.parent_scope
        WHERE a.parent_wf IS NOT NULL
          AND p.terminal_state IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM child_close_requests r
            WHERE r.workflow_id = p.workflow_id AND r.workflow_instance_key = p.workflow_instance_key AND r.workflow_instance_scope = p.workflow_instance_scope
              AND a.node_scope[1:COALESCE(array_length(r.target_prefix,1),0)] = r.target_prefix)
          AND EXISTS (
            SELECT 1 FROM unnest(a.node_prefixes) AS p(pfx)
            WHERE starts_with(a.signal_key, pfx))
      )
      SELECT sub_wf, sub_key, sub_scope, signal_key,
             node_wf AS source_wf, node_key AS source_key, node_scope AS source_scope,
             past, start_seq
      FROM ancestry
      WHERE depth > 0
    """

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
    *
    * === Closing correctness guarantees ===
    *
    * A child that can attach to a closing parent must be either covered by a
    * `child_close_requests` row the closing transition records or refused at
    * start, and a request must only ever match the children it names. Starting a
    * child upholds three guarantees that make this hold:
    *
    * 1. *Identity embedding* — the child's derived scope embeds the parent's
    *    generation and every enclosing region restart count
    *    (`ScopePath.deriveChildScope`), so a closure request naming an old
    *    generation or iteration can never match a child started later; old and
    *    new same-key children are distinct rows.
    * 2. *Attachment fencing* — only the parent's current-generation, non-terminal
    *    run may attach: the transaction locks the parent's own row first and
    *    refuses on generation mismatch or terminal parent, so nothing attaches
    *    under or after a closing transition.
    * 3. *No start into a closed scope* — a start is refused when a
    *    `child_close_requests` row of the parent already covers the derived
    *    scope, so a stale same-generation run cannot attach into an
    *    already-closed region interior.
    *
    * Consequence: every child that can attach to a closing parent is either
    * covered by a request the closing transition records or is refused at start,
    * and requests only ever match the children they name.
    */
  override def startChild[In, Out](
      workflow: Workflow[In, Out],
      childKey: WorkflowInstanceKey,
      input: In,
      parentClosePolicy: ParentClosePolicy,
      inheritSignals: SignalInheritance,
      inheritPastEvents: Boolean,
      enclosingScopePath: ScopePath
  )(using cacheable: Cacheable[In], lease: Lease): WorkflowInstance[In, Out] = {
    val parentId = lease.instanceId
    val parentGeneration = lease.generation
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
    val inheritPrefixes = permittedSignalPrefixes(inheritSignals)

    val existing = runTransaction {
      for {
        fence <- sql"""
          SELECT generation, terminal_state FROM workflow_instances
          WHERE workflow_id = ${parentId.workflowId} AND workflow_instance_key = ${parentId.workflowInstanceKey}
            AND workflow_instance_scope = ${parentId.scope}
          FOR UPDATE
        """.query[(Long, Option[String])].option
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
        covered <- sql"""
          SELECT 1 FROM child_close_requests r
          WHERE r.workflow_id = ${parentId.workflowId} AND r.workflow_instance_key = ${parentId.workflowInstanceKey}
            AND r.workflow_instance_scope = ${parentId.scope}
            AND $derivedScope[1:COALESCE(array_length(r.target_prefix,1),0)] = r.target_prefix
          LIMIT 1
        """.query[Int].option
        _ <- if (covered.isDefined) throw LeaseLostException(parentId) else ().pure[ConnectionIO]
        stored <- sql"""
          SELECT input FROM workflow_instances
          WHERE workflow_id = $childWorkflowId AND workflow_instance_key = $childKey AND workflow_instance_scope = $derivedScope
        """.query[String].option
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
                  inherited_signal_prefixes, inherit_past_events, inherited_events_start_sequence_id)
                VALUES ($childWorkflowId, $childKey, $derivedScope, $serializedInput, ${workflow.version}, 0,
                  ${parentId.workflowId}, ${parentId.workflowInstanceKey}, ${parentId.scope}, $policyStr,
                  $inheritPrefixes, $inheritPastEvents, $maxSeq)
                ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
              """.update.run
              
              _ <- if (inserted == 1)
                  upsertWakeupQ(childWorkflowId, childKey, derivedScope, clock.instant())
                else 
                  ().pure[ConnectionIO]
              
              raced <- 
                if (inserted == 0)
                  sql"""
                    SELECT input FROM workflow_instances
                    WHERE workflow_id = $childWorkflowId AND workflow_instance_key = $childKey AND workflow_instance_scope = $derivedScope
                  """.query[String].option
                else 
                  Option.empty[String].pure[ConnectionIO]
            } yield (inserted, raced)
        }
      } yield insertedAndStored
    }

    existing match {
      case (0, Some(stored)) if stored != serializedInput =>
        throw WorkflowInputConflictException(childInstanceId)
      case (0, Some(_)) =>
        warnOnIgnoredSignalInheritanceChange(
          childInstanceId, childWorkflowId, childKey, derivedScope, inheritPrefixes, inheritPastEvents
        )
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
    * upserted so the pending await is resumed and its cancellation check throws.
    */
  override def cancel(instanceId: WorkflowInstanceId): Unit = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val now = clock.instant()
    val unconsumed = runTransaction {
      for {
        row <- sql"""
          SELECT terminal_state, times_executed, lease_owner, lease_expires_at
          FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          FOR UPDATE
        """.query[(Option[String], Int, Option[String], Option[java.time.Instant])].option
        gathered <- row match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
            
          case Some((Some(_), _, _, _)) =>
            Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]

          case Some((None, timesExecuted, leaseOwner, leaseExpiresAt)) =>
            for {
              hasExecutionState <- hasExecutionStateQ(workflowId, key, scope)
              m <- if (timesExecuted == 0 && !hasExecutionState)
                finalizeCancelledWithoutLease(workflowId, key, scope)
              else
                for {
                  _ <- sql"""
                    UPDATE workflow_instances SET cancel_requested_at = $now
                    WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                      AND cancel_requested_at IS NULL
                  """.update.run
                  _ <- if (noLiveLease(leaseOwner, leaseExpiresAt, now))
                      upsertWakeupQ(workflowId, key, scope, now)
                    else 
                      ().pure[ConnectionIO]
                } yield Map.empty[SignalKey, Seq[String]]
            } yield m
        }
      } yield gathered
    }
    logDroppedUnconsumedSignals(instanceId, unconsumed)
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
  private def hasExecutionStateQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): ConnectionIO[Boolean] =
    for {
      steps <- sql"""
        SELECT 1 FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1
      """.query[Int].option
      
      signal <- sql"""
        SELECT 1 FROM workflow_signal_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1
      """.query[Int].option
      
      timer <- sql"""
        SELECT 1 FROM workflow_timer_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1
      """.query[Int].option
      
      completion <- sql"""
        SELECT 1 FROM workflow_completion_subscriptions
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1
      """.query[Int].option
      
      wakeup <- sql"""
        SELECT 1 FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope LIMIT 1
      """.query[Int].option
    } yield steps.isDefined || signal.isDefined || timer.isDefined || completion.isDefined || wakeup.isDefined

  /** Upserts a coalesced wakeup row due immediately, within the caller's
    * transaction.
    */
  private def upsertWakeupQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      now: java.time.Instant
  ): ConnectionIO[Unit] =
    sql"""
      INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
      VALUES ($workflowId, $key, $scope, $now, $now, 0)
      ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
    """.update.run.map(_ => ())

  /** The guarded `CANCELLED` terminal transition for an instance that has no
    * execution lease (finalized from a cancel request before first start): the
    * guarded status update plus the `WorkflowCompleted(Cancelled)` event, in one
    * transaction. The directly addressed `Signal` events are gathered (per key,
    * after cursors) and then deleted in the same transaction; because no
    * workflow definition is available on this path, the caller WARN-logs the
    * gathered map. Completion wakeups are produced asynchronously by the job
    * runner. The `Cancelled` outcome carries no user code, so it is encoded
    * directly with the runtime's framing.
    */
  private def finalizeCancelledWithoutLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): ConnectionIO[Map[SignalKey, Seq[String]]] = {
    val payload = Framing.write("cancelled")
    for {
      updated <- sql"""
        UPDATE workflow_instances
        SET terminal_state = 'cancelled', terminal_outcome = $payload
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          AND terminal_state IS NULL
      """.update.run
      
      unconsumed <- if (updated == 1)
          for {
            gathered <- gatherUnconsumedSignalsQ(workflowId, key, scope)
            _ <- deleteDirectSignalEventsQ(workflowId, key, scope)
            _ <- appendCompletedEvent(workflowId, key, scope, payload)
            _ <- terminalCleanupIO(workflowId, key, scope)
            _ <- insertChildCloseRequestQ(workflowId, key, scope, Vector.empty)
          } yield gathered
        else 
          Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]
    } yield unconsumed
  }

  /** Terminal cleanup of an instance's own scheduling and subscription rows:
    * its wakeup row and its signal/timer/completion subscriptions. Called inside
    * the terminal-transition transaction of every terminal state, so a terminal
    * instance never remains scheduled or subscribed.
    */
  private def terminalCleanupIO(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): ConnectionIO[Unit] =
    for {
      _ <- sql"""
        DELETE FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
      """.update.run
      _ <- deleteAllSubscriptionsQ(workflowId, key, scope)
    } yield ()

  /** Deletes the directly addressed `Signal` events of the instance at a terminal
    * transition. `TimerFired` and `WorkflowCompleted` events are retained
    * (subscribers and replays need them); consumers of the deleted `Signal`
    * events already hold cached Step results.
    */
  private def deleteDirectSignalEventsQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): ConnectionIO[Unit] =
    sql"""
      DELETE FROM workflow_events
      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
        AND event_kind = 'Signal'
    """.update.run.map(_ => ())

  /** Records a pending child close as a row in the dedicated
    * `child_close_requests` table: the parent's own identity plus the target
    * prefix of the children to close. `Vector.empty` targets every attached
    * child (a terminal parent closes all of them); a non-empty prefix targets
    * exactly the children whose derived scope lies under it, which keeps
    * targeting collision-free across generations and region iterations:
    * children started by a later generation or a later iteration carry markers
    * past the prefix and never match. The row is runtime bookkeeping in a
    * dedicated table — never a `workflow_steps` row — so no generic step
    * deletion (continueAsNew's generation erase, region-interior erases, fork
    * copies, resets) needs to exclude it. Delivery is asynchronous — the
    * child-close sweep and the run-start self-check apply the policies, and the
    * sweep deletes the row once no attached child matches it — so no parent
    * transaction ever locks another instance's rows.
    *
    * No row is written when no attached child matches the target (the common
    * childless case: terminal transitions of leaf instances, `continueAsNew`
    * and region restarts without children), so bookkeeping stays proportional
    * to actual pending work. The existence check is a read of the parent's own
    * children in the caller's transaction; the `startChild` generation fence
    * guarantees no matching child can attach afterwards. The composite primary
    * key deduplicates identical targets, so re-recording the same closure is
    * idempotent.
    */
  private def insertChildCloseRequestQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      target: Vector[String]
  ): ConnectionIO[Unit] = {
    val now = clock.instant()
    for {
      pending <- sql"""
        SELECT 1 FROM workflow_instances c
        WHERE c.parent_workflow_id = $workflowId AND c.parent_instance_key = $key AND c.parent_instance_scope = $scope
          AND c.workflow_instance_scope[1:${target.length}] = $target
        LIMIT 1
      """.query[Int].option
      
      _ <- if (pending.isDefined)
          sql"""
            INSERT INTO child_close_requests (workflow_id, workflow_instance_key, workflow_instance_scope, target_prefix, created_at)
            VALUES ($workflowId, $key, $scope, $target, $now)
            ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, target_prefix) DO NOTHING
          """.update.run
        else 
          ().pure[ConnectionIO]
    } yield ()
  }

  /** Closes one child per its `ParentClosePolicy` (see [[insertChildCloseRequestQ]]
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
  private def closeOneChildQ(
      child: (WorkflowId, WorkflowInstanceKey, ScopePath, Option[String], Option[String], Int, Option[String], Option[java.time.Instant])
  ): ConnectionIO[Map[SignalKey, Seq[String]]] = {
    val now = clock.instant()
    val (cwf, ckey, cscope, policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt) = child
    
    val clearPointer = sql"""
      UPDATE workflow_instances
      SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_instance_scope = NULL
      WHERE workflow_id = $cwf AND workflow_instance_key = $ckey AND workflow_instance_scope = $cscope
    """.update.run.map(_ => ())
    
    val deliver = policy match {
      case Some("abandon") => Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]
      
      case _ => terminal match {
        case Some(_) => Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]

        case None =>
          if (timesExecuted == 0)
            finalizeCancelledWithoutLease(cwf, ckey, cscope)
          else for {
            _ <- sql"""
              UPDATE workflow_instances SET cancel_requested_at = $now
              WHERE workflow_id = $cwf AND workflow_instance_key = $ckey AND workflow_instance_scope = $cscope
                AND cancel_requested_at IS NULL
            """.update.run
            _ <- if (noLiveLease(leaseOwner, leaseExpiresAt, now))
                upsertWakeupQ(cwf, ckey, cscope, now)
              else 
                ().pure[ConnectionIO]
          } yield Map.empty[SignalKey, Seq[String]]
      }
    }
    
    for {
      _ <- clearPointer
      unconsumed <- deliver
    } yield unconsumed
  }

  /** The child-close delivery sweep (the asynchronous half of
    * [[insertChildCloseRequestQ]]): applies every pending closure whose target
    * still matches attached children, and detaches children whose parent row is
    * gone (a hard-deleted parent detaches its children; it never cancels them,
    * matching the behavior of the inheritance walk, which also stops at a
    * missing parent). Candidates are read without locks; each child is closed in
    * its own short transaction that locks only that child's row and re-verifies
    * the condition, so the sweep never locks rows of other instances and is
    * idempotent under concurrent sweeps and run-start self-checks. Finally,
    * `child_close_requests` rows that no longer match any attached child are
    * deleted (exhausted records). Returns the number of children closed.
    */
  private[atomicflow] def closePendingChildClosures(limit: Int): Int = {
    val candidates = withDeadlockRetry {
      runTransaction {
        for {
          closable <-
            sql"""
              SELECT DISTINCT c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
              FROM workflow_instances c
              JOIN child_close_requests r
                ON r.workflow_id = c.parent_workflow_id AND r.workflow_instance_key = c.parent_instance_key
                   AND r.workflow_instance_scope = c.parent_instance_scope
              WHERE c.parent_workflow_id IS NOT NULL
                AND c.workflow_instance_scope[1:COALESCE(array_length(r.target_prefix,1),0)] = r.target_prefix
              ORDER BY c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
              LIMIT $limit
            """.query[(WorkflowId, WorkflowInstanceKey, ScopePath)].to[Vector]
            
          orphans <-
            sql"""
              SELECT c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
              FROM workflow_instances c
              WHERE c.parent_workflow_id IS NOT NULL
                AND NOT EXISTS (
                  SELECT 1 FROM workflow_instances p
                  WHERE p.workflow_id = c.parent_workflow_id
                    AND p.workflow_instance_key = c.parent_instance_key
                    AND p.workflow_instance_scope = c.parent_instance_scope)
              ORDER BY c.workflow_id, c.workflow_instance_key, c.workflow_instance_scope
              LIMIT $limit
            """.query[(WorkflowId, WorkflowInstanceKey, ScopePath)].to[Vector]
        } yield closable.map(("closable", _)) ++ orphans.map(("orphan", _))
      }
    }
    var closed = 0
    candidates.foreach { case (kind, (wf, k, s)) =>
      val (done, unconsumed) =
        withDeadlockRetry(runTransaction(closeOnePendingChildQ(wf, k, s, orphan = kind == "orphan")))
      
      if (unconsumed.nonEmpty) 
        logDroppedUnconsumedSignals(WorkflowInstanceId(wf, k, s), unconsumed)
      
      if (done) 
        closed += 1
    }
    val removed = withDeadlockRetry {
      runTransaction {
        sql"""
          DELETE FROM child_close_requests r
          WHERE NOT EXISTS (
            SELECT 1 FROM workflow_instances c
            WHERE c.parent_workflow_id = r.workflow_id
              AND c.parent_instance_key = r.workflow_instance_key
              AND c.parent_instance_scope = r.workflow_instance_scope
              AND c.workflow_instance_scope[1:COALESCE(array_length(r.target_prefix,1),0)] = r.target_prefix)
        """.update.run
      }
    }
    val total = closed + removed
    if (total > 0) 
      log.info(s"Child-close sweep closed $closed children and removed $removed exhausted records")
    
    closed
  }

  /** Closes one sweep candidate in its own transaction, locking only that
    * child's row. Re-verifies under the lock that the parent pointer is still
    * set and — for a closable candidate — that a `child_close_requests` row of
    * that parent still matches this child's scope, or — for an orphan candidate
    * — that the parent row is still gone; a raced-away condition makes this a
    * no-op (returns `false`).
    */
  private def closeOnePendingChildQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      orphan: Boolean
  ): ConnectionIO[(Boolean, Map[SignalKey, Seq[String]])] =
    for {
      row <- sql"""
        SELECT parent_workflow_id, parent_instance_key, parent_instance_scope, parent_close_policy, terminal_state, times_executed, lease_owner, lease_expires_at
        FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
        FOR UPDATE
      """.query[
              (Option[String], Option[String], Option[ScopePath], Option[String], Option[String], Int, Option[String], Option[java.time.Instant])
            ].option
      
      done <- row match {
        case Some((Some(pwf), Some(pkey), Some(pscope), policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt)) =>
          if (orphan) {
            for {
              stillGone <- sql"""
                SELECT 1 FROM workflow_instances
                WHERE workflow_id = $pwf AND workflow_instance_key = $pkey AND workflow_instance_scope = $pscope
              """.query[Int].option
              _ <- if (stillGone.isEmpty)
                sql"""
                  UPDATE workflow_instances
                  SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_instance_scope = NULL
                  WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                """.update.run
              else ().pure[ConnectionIO]
            } yield (stillGone.isEmpty, Map.empty[SignalKey, Seq[String]])
          } else {
            for {
              matched <- sql"""
                SELECT 1 FROM child_close_requests r
                WHERE r.workflow_id = $pwf AND r.workflow_instance_key = $pkey AND r.workflow_instance_scope = $pscope
                  AND $scope[1:COALESCE(array_length(r.target_prefix,1),0)] = r.target_prefix
                LIMIT 1
              """.query[Int].option
              unconsumed <- if (matched.isDefined)
                closeOneChildQ((workflowId, key, scope, policy, terminal, timesExecuted, leaseOwner, leaseExpiresAt))
              else Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]
            } yield (matched.isDefined, unconsumed)
          }
          
        case _ => (false, Map.empty[SignalKey, Seq[String]]).pure[ConnectionIO]
      }
    } yield done

  /** The run-start self-check (the synchronous half of [[insertChildCloseRequestQ]]
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
      scope: ScopePath
  ): (Boolean, Map[SignalKey, Seq[String]]) = {
    val now = clock.instant()
    runTransaction {
      for {
        row <- sql"""
          SELECT times_executed, terminal_state, parent_workflow_id, parent_instance_key, parent_instance_scope, parent_close_policy
          FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          FOR UPDATE
        """.query[
                (Int, Option[String], Option[String], Option[String], Option[ScopePath], Option[String])
              ].option
        
        abort <- row match {
          case Some((timesExecuted, None, Some(pwf), Some(pkey), Some(pscope), policy)) =>
            val clearPointer =
              sql"""
                UPDATE workflow_instances
                SET parent_workflow_id = NULL, parent_instance_key = NULL, parent_instance_scope = NULL
                WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
              """.update.run
              
            for {
              parentUsable <- sql"""
                SELECT 1 FROM workflow_instances p
                WHERE p.workflow_id = $pwf AND p.workflow_instance_key = $pkey AND p.workflow_instance_scope = $pscope
              """.query[Int].option
              
              result <- parentUsable match {
                // A hard-deleted parent detaches its child; it never cancels.
                case None => clearPointer.as((false, Map.empty[SignalKey, Seq[String]]))

                case Some(_) =>
                  for {
                    matched <- sql"""
                      SELECT 1 FROM child_close_requests r
                      WHERE r.workflow_id = $pwf AND r.workflow_instance_key = $pkey AND r.workflow_instance_scope = $pscope
                        AND $scope[1:COALESCE(array_length(r.target_prefix,1),0)] = r.target_prefix
                      LIMIT 1
                    """.query[Int].option
                    
                    r <- matched match {
                      case None => (false, Map.empty[SignalKey, Seq[String]]).pure[ConnectionIO]

                      case Some(_) =>
                        policy match {
                          case Some("abandon") => clearPointer.as((false, Map.empty[SignalKey, Seq[String]]))

                          case _ =>
                            if (timesExecuted == 1) {
                              // The first execution of a never-started child: the
                              // body never runs; finalize CANCELLED and abort.
                              for {
                                _ <- clearPointer
                                m <- finalizeCancelledWithoutLease(workflowId, key, scope)
                              } yield (true, m)
                            } else {
                              // A started child: deliver the cooperative flag;
                              // the next cancellation checkpoint throws.
                              for {
                                _ <- clearPointer
                                _ <- sql"""
                                  UPDATE workflow_instances SET cancel_requested_at = $now
                                  WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                                    AND cancel_requested_at IS NULL
                                """.update.run
                              } yield (false, Map.empty[SignalKey, Seq[String]])
                            }
                        }
                    }
                  } yield r
              }
            } yield result
            
          case _ => (false, Map.empty[SignalKey, Seq[String]]).pure[ConnectionIO]
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
      sql"""
        SELECT 1 FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
      """.query[Int].option
    }
    if (exists.isEmpty)
      throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
    val (_, unconsumed) = escalateTerminated(instanceId)
    logDroppedUnconsumedSignals(instanceId, unconsumed)
    ()
  }

  /** The guarded `continueAsNew` in-place transition, committed in ONE
    * transaction under the run's lease: gather the directly addressed `Signal`
    * events (per key, after cursors), erase the old generation's execution
    * records (Step rows, subscriptions, and wakeup), keep the exact-key signal
    * cursors, record a `child_close_requests` row targeting exactly the
    * children of the closing generation (delivery is asynchronous; see
    * [[insertChildCloseRequestQ]]), delete the gathered `Signal` events,
    * increment the generation, install the new input, and upsert the successor's
    * wakeup. Fenced: if the lease no longer belongs to this run, no rows are
    * affected and [[LeaseLostException]] is thrown.
    *
    * Returns the gathered unconsumed-signal map (empty when nothing was
    * visible); the caller invokes the `onUnconsumedSignals` listener with it
    * after this transaction commits.
    */
  private def continueAsNewTransition(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      worker: String,
      token: Long,
      newSerializedInput: String
  ): Map[SignalKey, Seq[String]] = {
    val now = clock.instant()
    val res = runTransaction {
      for {
        locked <- sql"""
          SELECT generation FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
            AND lease_owner = $worker AND fencing_token = $token
          FOR UPDATE
        """.query[Long].option
        
        done <- locked match {
          case None => Option.empty[Map[SignalKey, Seq[String]]].pure[ConnectionIO]

          case Some(generation) =>
            for {
              unconsumed <- gatherUnconsumedSignalsQ(workflowId, key, scope)
              _ <- sql"""
                DELETE FROM workflow_steps
                WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
              """.update.run
              _ <- terminalCleanupIO(workflowId, key, scope)
              _ <- insertChildCloseRequestQ(workflowId, key, scope,
                                         scope.segments ++ Vector(workflowId, s"$key@$generation"))
              _ <- deleteDirectSignalEventsQ(workflowId, key, scope)
              _ <- sql"""
                UPDATE workflow_instances
                SET input = $newSerializedInput, generation = generation + 1
                WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
              """.update.run
              _ <- upsertWakeupQ(workflowId, key, scope, now)
            } yield Some(unconsumed)
        }
      } yield done
    }
    
    res match {
      case Some(unconsumed) => unconsumed
      case None             => throw LeaseLostException(WorkflowInstanceId(workflowId, key, scope))
    }
  }

  /** The guarded `TERMINATED` terminal transition, shared by the public
    * [[terminate]] and the cancellation-escalation sweep. Row-locks the
    * instance; a missing or already-terminal instance is a no-op (returns
    * `(false, Map.empty)`); otherwise the guarded status update plus the
    * `WorkflowCompleted(Terminated)` event, lease revocation (fencing-token
    * bump and clearing owner/expiry), and terminal cleanup commit in one
    * transaction. The directly addressed `Signal` events are gathered (per key,
    * after cursors) and then deleted in the same transaction; because no
    * workflow definition is available on this path, the caller WARN-logs the
    * gathered map. Completion wakeups are produced asynchronously by the job
    * runner. Idempotent: a concurrent winner makes a later caller a no-op.
    */
  private[atomicflow] def escalateTerminated(
      instanceId: WorkflowInstanceId
  ): (Boolean, Map[SignalKey, Seq[String]]) = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val payload = Framing.write("terminated")
    
    runTransaction {
      for {
        terminal <- sql"""
          SELECT terminal_state FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          FOR UPDATE
        """.query[Option[String]].option
        
        updated <- terminal match {
          case None => (false, Map.empty[SignalKey, Seq[String]]).pure[ConnectionIO]
          
          case Some(Some(_)) => (false, Map.empty[SignalKey, Seq[String]]).pure[ConnectionIO]

          case Some(None) =>
            for {
              u <- sql"""
                UPDATE workflow_instances
                SET terminal_state = 'terminated', terminal_outcome = $payload,
                    lease_owner = NULL, lease_expires_at = NULL,
                    fencing_token = fencing_token + 1
                WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                  AND terminal_state IS NULL
              """.update.run
              unconsumed <- if (u == 1)
                  for {
                    gathered <- gatherUnconsumedSignalsQ(workflowId, key, scope)
                    _ <- deleteDirectSignalEventsQ(workflowId, key, scope)
                    _ <- appendCompletedEvent(workflowId, key, scope, payload)
                    _ <- terminalCleanupIO(workflowId, key, scope)
                    _ <- insertChildCloseRequestQ(workflowId, key, scope, Vector.empty)
                  } yield gathered
                else 
                  Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]
            } yield (u == 1, unconsumed)
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
  private[atomicflow] def fireTimerSubscriptionQ(
      timerId: java.util.UUID,
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): ConnectionIO[Boolean] = {
    val now = clock.instant()
    for {
      locked <- sql"""
        SELECT deadline FROM workflow_timer_subscriptions
        WHERE timer_id = $timerId
        FOR UPDATE
      """.query[java.time.Instant].option
      
      fired <- locked match {
        case None => false.pure[ConnectionIO]

        case Some(_) =>
          for {
            exists <- sql"""
              SELECT 1 FROM workflow_events
              WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
                AND event_kind = 'TimerFired' AND event_key = ${timerId.toString}
            """.query[Int].option
            
            _ <- if (exists.isEmpty)
                for {
                  _ <- appendEvent(workflowId, key, scope, "TimerFired", timerId.toString, "")
                  _ <- upsertWakeupQ(workflowId, key, scope, now)
                } yield ()
              else 
                ().pure[ConnectionIO]
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
    val now = clock.instant()
    runTransaction {
      for {
        updated <- sql"""
          UPDATE workflow_instances
          SET lease_owner = NULL, lease_expires_at = NULL
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
            AND lease_owner IS NOT NULL AND terminal_state IS NULL
        """.update.run
        
        _ <- if (updated == 1) upsertWakeupQ(workflowId, key, scope, now)
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
        sql"SELECT dispatched_through FROM workflow_event_dispatch WHERE id = 1 FOR UPDATE SKIP LOCKED".query[Long].option
          .flatMap {
            case None => 0.pure[ConnectionIO]
            case Some(through) =>
              sql"""
                SELECT sequence_id, event_kind, workflow_id, workflow_instance_key, workflow_instance_scope, event_key
                FROM workflow_events WHERE sequence_id > $through
                ORDER BY sequence_id LIMIT $batchSize
              """.query[
                  (Long, String, WorkflowId, WorkflowInstanceKey, ScopePath, String)
                ].to[Vector].flatMap { events =>
                  events.traverse_ { case (_, kind, wf, k, s, ek) =>
                    kind match {
                      case "Signal" =>
                        wakeMatchingSubscribers(wf, k, s, ek) *> wakeSignalInheritors(wf, k, s, ek)
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
    * wakeups inserted. Timer subscriptions are excluded (the timer
    * sweep and the instance-row lock cover them).
    */
  private[atomicflow] def scanAllSubscriptions(): Int = {
    val now = clock.instant()
    withDeadlockRetry {
      runTransaction {
        for {
          direct <- sql"""
            INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
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
            ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
          """.update.run
          
          inherited <- (
            fr"""
            INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
            SELECT DISTINCT a.sub_wf, a.sub_key, a.sub_scope, $now, $now, 0
            FROM (
            """ ++ inheritedSignalSources(signalSubscriberCandidates(Fragment.empty)) ++
            fr"""
              ) a
              WHERE EXISTS (
                  SELECT 1 FROM workflow_events e
                  WHERE e.event_kind = 'Signal' AND e.event_key = a.signal_key
                    AND e.workflow_id = a.source_wf AND e.workflow_instance_key = a.source_key AND e.workflow_instance_scope = a.source_scope
                    AND e.sequence_id > COALESCE((
                      SELECT c.sequence_id FROM signal_cursor c
                      WHERE c.workflow_id = a.sub_wf AND c.workflow_instance_key = a.sub_key
                        AND c.workflow_instance_scope = a.sub_scope AND c.signal_key = a.signal_key), 0)
                    AND (a.past OR a.start_seq IS NULL OR e.sequence_id > a.start_seq)
                )
              ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
            """).update.run
          
          completion <- sql"""
            INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
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
            ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
          """.update.run
        } yield direct + inherited + completion
      }
    }
  }

  /** Runs `body` while holding the instance's execution lease: acquires the
    * lease, bumps `times_executed`, re-reads the instance row under the fence,
    * applies the run-start parent-close self-check, and always releases the
    * lease afterwards. Returns `AlreadyTerminal` without running `body` when the
    * instance is already terminal (before acquisition, after acquisition, or
    * when a pending parent close finalizes a never-started child).
    */
  override def withLease[A](instance: WorkflowInstanceId, acquireTimeout: FiniteDuration)(
      body: Lease ?=> A
  ): LeasedRunResult[A] = {
    val workflowId = instance.workflowId
    val key = instance.workflowInstanceKey
    val scope = instance.scope

    clearInterrupt()

    val initial = readInstance(instance).getOrElse(
      throw new WorkflowNotFoundException(s"Workflow instance not found: $instance")
    )
    initial.terminalState match {
      case Some(_) => LeasedRunResult.AlreadyTerminal(initial)

      case None =>
        val token = acquireLease(workflowId, key, scope, workerId, leaseDuration, acquireTimeout) match {
          case Some(t) => t
          case None    => throw LeaseUnavailableException(instance)
        }
        try {
          leaseUnderAcquisition(instance, workerId, token) match {
            case Left(record) => LeasedRunResult.AlreadyTerminal(record)
            case Right(lease) => LeasedRunResult.Ran(body(using lease))
          }
        } finally {
          releaseLeaseIfStillOwned(instance, workerId, token)
        }
    }
  }

  /** The shared lease-acquisition follow-on: bump `times_executed` (+
    * `last_run_at`) fenced by `worker`/`token`, re-read the instance row under
    * the same fence, and apply the run-start parent-close self-check. Returns
    * `Left(record)` when the instance is terminal (an already-terminal row, or a
    * never-started child finalized `CANCELLED` by the self-check), otherwise
    * `Right(lease)` carrying the freshly read record. Throws
    * [[LeaseLostException]] when the fenced bump or re-read affects no row.
    */
  private def leaseUnderAcquisition(
      instance: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Either[InstanceRecord, PostgresCurrentExecution] = {
    val workflowId = instance.workflowId
    val key = instance.workflowInstanceKey
    val scope = instance.scope
    val now = clock.instant()
    val bumped = runTransaction {
      sql"""
        UPDATE workflow_instances
        SET times_executed = times_executed + 1, last_run_at = $now
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          AND lease_owner = $worker AND fencing_token = $token
      """.update.run
    }
    
    if (bumped != 1) 
      throw LeaseLostException(instance)

    // Re-read the instance row now that we hold the lease. A caller-thread run
    // may have waited out a live lease, during which a concurrent
    // continueAsNew/reset replaced the input and bumped the generation; the
    // pre-acquisition read would then be stale. Reading fenced by our owner+token
    // guarantees we observe the current row (or detect we lost the lease).
    val fresh = readFencedInstanceRecord(instance, worker, token).getOrElse(
      throw LeaseLostException(instance)
    )
    
    fresh.terminalState match {
      case Some(_) => Left(fresh)
      case None =>
        val parentCloseAbort =
          if (fresh.parentId.isDefined) {
            val (aborted, unconsumed) = parentClosureSelfCheck(workflowId, key, scope)
            logDroppedUnconsumedSignals(instance, unconsumed)
            aborted
          } 
          else false
        
        if (parentCloseAbort) {
          log.info(s"Workflow instance $instance cancelled by pending parent close before first execution")
          Left(readInstance(instance).getOrElse(throw LeaseLostException(instance)))
        } else {
          log.debug(s"Running workflow instance $instance (fencingToken=$token)")
          Right(new PostgresCurrentExecution(worker, token, workflowId, key, scope, fresh.generation, fresh))
        }
    }
  }

  /** Reads the full persisted state of an instance as an [[InstanceRecord]]. */
  override def readInstance(instanceId: WorkflowInstanceId): Option[InstanceRecord] =
    runTransaction {
      (InstanceSelect ++ fr"""
        WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey}
        AND workflow_instance_scope = ${instanceId.scope}
      """).query[InstanceRow].option
    }.map(toInstanceRecord(instanceId, _))

  /** Reads the full persisted state of an instance fenced by `worker`/`token`.
    * The lease-fence predicate is part of the WHERE clause, so a row whose lease
    * no longer belongs to this run matches nothing.
    */
  private def readFencedInstanceRecord(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Option[InstanceRecord] =
    runTransaction {
      (InstanceSelect ++ fr"""
        WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey}
        AND workflow_instance_scope = ${instanceId.scope}
        AND lease_owner = $worker AND fencing_token = $token
      """).query[InstanceRow].option
    }.map(toInstanceRecord(instanceId, _))

  /** The columns backing an [[InstanceRecord]], in `InstanceSelect` order. */
  private type InstanceRow = (
      String,
      Long,
      Long,
      Option[String],
      Option[String],
      Option[String],
      Option[String],
      Option[ScopePath],
      java.time.Instant,
      Option[java.time.Instant],
      Int
  )

  private val InstanceSelect: Fragment =
    fr"""
      SELECT input, workflow_version_at_creation, generation, terminal_state, terminal_outcome,
      parent_workflow_id, parent_instance_key, parent_instance_scope, created_at, last_run_at, times_executed
      FROM workflow_instances
    """

  private def toInstanceRecord(instanceId: WorkflowInstanceId, row: InstanceRow): InstanceRecord = {
    val (input, version, generation, terminal, outcome, parentWf, parentKey, parentScope, createdAt, lastRunAt, timesExecuted) =
      row
    val parentId = parentWf.map(pwf => WorkflowInstanceId(pwf, parentKey.getOrElse(""), parentScope.getOrElse(ScopePath.empty)))
    InstanceRecord(
      instanceId = instanceId,
      input = SerializedValue.of(input),
      versionAtCreation = version,
      generation = generation,
      terminalState = terminal.map(terminalStateEnumFromStr),
      terminalOutcome = outcome.map(SerializedValue.of),
      parentId = parentId,
      createdAt = createdAt,
      lastRunAt = lastRunAt,
      timesExecuted = timesExecuted
    )
  }

  /** Runs an instance whose lease is already held by the job runner's claim
    * (worker + fencing token supplied), skipping acquisition. Uses the same
    * body-execution code path as the public run API.
    */
  private[atomicflow] def runClaimedInstance(
      workflow: Workflow[?, ?],
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[?] =
    workflow match {
      case wf: Workflow[i, o] =>
        clearInterrupt()
        val initial = readInstance(instanceId).getOrElse(
          throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
        )
        initial.terminalState match {
          case Some(_) => decodeTerminal(wf, initial)

          case None =>
            try {
              leaseUnderAcquisition(instanceId, worker, token) match {
                case Left(record) => decodeTerminal(wf, record)
                case Right(lease) => executeUnderLease(wf)(using cacheableThrowable, lease)
              }
            } finally {
              releaseLeaseIfStillOwned(instanceId, worker, token)
            }
        }
    }

  /** Records the run's terminal outcome under the lease. The guarded terminal
    * transition wins only if the instance is still non-terminal and the lease is
    * still ours; otherwise the already-terminal row is read back and returned.
    */
  override def completeInstance(outcome: TerminalOutcome)(using lease: Lease): CompletionResult = {
    val (state, payload) = outcome match {
      case TerminalOutcome.Completed(p) => ("completed", p.raw)
      case TerminalOutcome.Failed(p)    => ("failed", p.raw)
      case TerminalOutcome.Cancelled    => ("cancelled", Framing.write("cancelled"))
      case TerminalOutcome.Terminated   => ("terminated", Framing.write("terminated"))
    }
    val (updated, unconsumed) = terminalTransitionAndEvent(
      lease.workflowId,
      lease.instanceKey,
      lease.instanceScope,
      lease.workerId,
      lease.fencingToken,
      state,
      payload
    )
    if (updated == 1) CompletionResult.Applied(unconsumed)
    else
      CompletionResult.AlreadyTerminal(
        readInstance(lease.instanceId).getOrElse(throw LeaseLostException(lease.instanceId))
      )
  }

  /** Commits a continue-as-new transition with `nextInput` under the lease and
    * returns the instance's unconsumed signals. Core invokes the
    * `onUnconsumedSignals` listener with the returned map.
    */
  override def continueAsNew(nextInput: SerializedValue)(using lease: Lease): Map[SignalKey, Seq[String]] =
    continueAsNewTransition(
      lease.workflowId,
      lease.instanceKey,
      lease.instanceScope,
      lease.workerId,
      lease.fencingToken,
      nextInput.raw
    )

  /** One conditional lease-acquire attempt as a `ConnectionIO`, so a caller may
    * run it inside its own transaction (the job runner's claim). Returns the new
    * fencing token on success, `None` if the lease is held by a live owner or the
    * instance is terminal. The acquire and the fencing-token read-back are a
    * single `RETURNING` statement.
    */
  private[atomicflow] def tryAcquireLeaseQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      worker: String,
      leaseDuration: FiniteDuration
  ): ConnectionIO[Option[Long]] = {
    val now = clock.instant()
    val expires = leaseExpiry(now, leaseDuration)
    sql"""
      UPDATE workflow_instances
      SET lease_owner = $worker, fencing_token = fencing_token + 1, lease_expires_at = $expires
      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
        AND terminal_state IS NULL
        AND (lease_owner IS NULL OR lease_expires_at <= $now)
      RETURNING fencing_token
    """.query[Long].option
  }

  /** Bounded poll of the conditional lease acquire, every 100ms up to
    * `leaseAcquireTimeout`.
    */
  private def acquireLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      worker: String,
      leaseDuration: FiniteDuration,
      leaseAcquireTimeout: FiniteDuration
  ): Option[Long] = {
    val pollIntervalMillis = 100L
    val deadlineNanos = System.nanoTime() + leaseAcquireTimeout.toNanos
    var acquired: Option[Long] = None
    while (acquired.isEmpty && System.nanoTime() < deadlineNanos) {
      acquired = runTransaction(tryAcquireLeaseQ(workflowId, key, scope, worker, leaseDuration))
      if (acquired.isEmpty && System.nanoTime() < deadlineNanos)
        Thread.sleep(pollIntervalMillis)
    }
    acquired
  }

  /** Clears the lease if the worker and fencing token still match. The run
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
      sql"""
        UPDATE workflow_instances
        SET lease_owner = NULL, lease_expires_at = NULL
        WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey} AND workflow_instance_scope = ${instanceId.scope}
          AND lease_owner = $worker AND fencing_token = $token
      """.update.run
    }

  /** The guarded terminal transition, atomic with the `WorkflowCompleted` event
    * append, the gather of this instance's unconsumed directly addressed
    * `Signal` events, and their deletion: the guarded
    * `terminal_state`/`terminal_outcome` update, the gather, the event append,
    * and the `Signal` deletion commit together in one transaction. Completion
    * wakeups are produced asynchronously by the job runner. Wins only if the
    * instance is not already terminal and the lease is still ours. `TimerFired`
    * and `WorkflowCompleted` events are retained (subscribers and replays need
    * them); consumers of the deleted `Signal` events already hold cached Step
    * results.
    *
    * Returns `(rowsUpdated, unconsumed)` where `rowsUpdated` is 1 if this writer
    * won the transition and 0 if another writer already made it terminal; the
    * unconsumed map is only meaningful when `rowsUpdated == 1`. The caller
    * invokes the `onUnconsumedSignals` listener with it after commit.
    */
  private def terminalTransitionAndEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      worker: String,
      token: Long,
      state: String,
      payload: String
  ): (Int, Map[SignalKey, Seq[String]]) =
    runTransaction {
      for {
        updated <- sql"""
          UPDATE workflow_instances
          SET terminal_state = $state, terminal_outcome = $payload
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
            AND terminal_state IS NULL AND lease_owner = $worker AND fencing_token = $token
        """.update.run
        
        unconsumed <- if (updated == 1)
            for {
              gathered <- gatherUnconsumedSignalsQ(workflowId, key, scope)
              _ <- deleteDirectSignalEventsQ(workflowId, key, scope)
              _ <- appendCompletedEvent(workflowId, key, scope, payload)
              _ <- terminalCleanupIO(workflowId, key, scope)
              _ <- insertChildCloseRequestQ(workflowId, key, scope, Vector.empty)
            } yield gathered
          else 
            Map.empty[SignalKey, Seq[String]].pure[ConnectionIO]
      } yield (updated, unconsumed)
    }

  /** Gathers the currently visible unconsumed `Signal` events of the instance,
    * as a `ConnectionIO` so it runs inside the caller's terminal/continue-as-new
    * transaction (which holds the instance row lock): for every key, the events
    * after that key's shared cursor (cursor absent means from the first event),
    * in `sequenceId` order, as their raw serialized payloads. Keys with no
    * events after their cursor are absent from the result.
    */
  private def gatherUnconsumedSignalsQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath
  ): ConnectionIO[Map[SignalKey, Seq[String]]] =
    sql"""
      SELECT event_key, payload FROM workflow_events
      WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
        AND event_kind = 'Signal'
        AND sequence_id > (
          SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
          WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
            AND signal_key = workflow_events.event_key
        )
      ORDER BY sequence_id
    """.query[(String, String)].to[Vector].map(_.groupMap(_._1)(_._2))

  /** WARN-logs unconsumed signals gathered on a terminal path that has no
    * workflow definition available (terminate, cancellation escalation, and the
    * cancel-before-first-start finalization), so they are not silently dropped;
    * the `onUnconsumedSignals` listener could not run.
    */
  private[atomicflow] def logDroppedUnconsumedSignals(
      instanceId: WorkflowInstanceId,
      unconsumed: Map[SignalKey, Seq[String]]
  ): Unit = {
    if (unconsumed.nonEmpty)
      log.warn(s"Workflow instance $instanceId reached a terminal transition with unconsumed signals, " +
          s"but no workflow definition is available on this path to run the onUnconsumedSignals listener; dropped: $unconsumed")
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
      scope: ScopePath
  ): ConnectionIO[Unit] = {
    val now = clock.instant()
    for {
      subscribers <- sql"""
        SELECT s.workflow_id, s.workflow_instance_key, s.workflow_instance_scope
        FROM workflow_completion_subscriptions s
        JOIN workflow_instances i
          ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
        WHERE s.completed_workflow_id = $workflowId AND s.completed_workflow_instance_key = $key AND s.completed_workflow_instance_scope = $scope
          AND i.terminal_state IS NULL
      """.query[(WorkflowId, WorkflowInstanceKey, ScopePath)].to[Vector]
      
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
      scope: ScopePath,
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
      scope: ScopePath,
      kind: String,
      eventKey: String,
      payload: String
  ): ConnectionIO[Unit] = {
    val createdAt = clock.instant()
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
    * if it is already terminal (`terminal_state IS NOT NULL`), else append the
    * event (payload from the signal's [[Cacheable]]). The terminal transition
    * that deletes the addressed events commits under the same row lock, so a
    * signal either commits first — and is thereby included in the gathered
    * unconsumed map — or is rejected as already-completed. The sender only
    * appends; wakeups are produced asynchronously by the job runner. Does not
    * acquire the execution lease.
    */
  override def sendSignal[A: Cacheable as cacheable](
      workflowInstanceId: WorkflowInstanceId,
      key: SignalKey,
      value: A
  ): SignalSendResult = {
    val workflowId = workflowInstanceId.workflowId
    val instanceKey = workflowInstanceId.workflowInstanceKey
    val scope = workflowInstanceId.scope
    val payload = cacheable.write(value)
    runTransaction {
      for {
        terminal <- sql"""
          SELECT terminal_state FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_key = $instanceKey AND workflow_instance_scope = $scope
          FOR UPDATE
        """.query[Option[String]].option
        result <- terminal match {
          case None =>
            throw new WorkflowNotFoundException(s"Workflow instance not found: $workflowInstanceId")
          case Some(Some(_)) => SignalSendResult.InstanceAlreadyCompleted.pure[ConnectionIO]

          case Some(None) =>
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
      scope: ScopePath,
      signalKey: SignalKey
  ): ConnectionIO[Unit] = {
    val now = clock.instant()
    for {
      has <- sql"""
        SELECT 1
        FROM workflow_signal_subscriptions s
        JOIN workflow_instances i
          ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
        WHERE s.workflow_id = $workflowId AND s.workflow_instance_key = $key AND s.workflow_instance_scope = $scope AND s.signal_key = $signalKey
          AND i.terminal_state IS NULL
        LIMIT 1
      """.query[Int].option
      
      _ <- if (has.isDefined)
          sql"""
            INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
            VALUES ($workflowId, $key, $scope, $now, $now, 0)
            ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
          """.update.run.map(_ => ())
        else 
          ().pure[ConnectionIO]
    } yield ()
  }

  /** Upserts a wakeup for every non-terminal subscriber of `signalKey` whose
    * signal-inheritance sources ([[inheritedSignalSources]]) include the
    * addressed instance, so one committed send reaches all descendants that
    * inherit it. The awaiting descendant re-checks eligibility when it runs.
    * Waking a superset would be harmless, but the shared source query already
    * applies the exact policy, so no superset is needed. Called by the
    * best-effort watermark dispatcher; the full subscription scan covers the
    * same case independently.
    */
  private def wakeSignalInheritors(
      sourceWorkflowId: WorkflowId,
      sourceInstanceKey: WorkflowInstanceKey,
      sourceScope: ScopePath,
      signalKey: SignalKey
  ): ConnectionIO[Unit] = {
    val now = clock.instant()
    (fr"""
      INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, workflow_instance_scope, created_at, scheduled_at, attempts)
      SELECT DISTINCT a.sub_wf, a.sub_key, a.sub_scope, $now, $now, 0
      FROM (
    """ ++ inheritedSignalSources(signalSubscriberCandidates(fr" AND s.signal_key = $signalKey")) ++
    fr"""
      ) a
      WHERE a.source_wf = $sourceWorkflowId AND a.source_key = $sourceInstanceKey AND a.source_scope = $sourceScope
      ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope) DO NOTHING
    """).update.run.map(_ => ())
  }

  override def readStep(
      instanceId: WorkflowInstanceId,
      stepId: StepId,
      stepVersion: Long
  ): Option[StepRow] =
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
      scope: ScopePath,
      stepId: String,
      stepScopePath: ScopePath,
      stepVersion: Long
  ): Option[StepRow] =
    runTransaction {
      sql"""
        SELECT step_kind, state_kind, state_payload, retry_deadline, input_fingerprints, expires_at FROM workflow_steps
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
          AND step_id = $stepId AND step_scope_within_instance = $stepScopePath AND step_version = $stepVersion
      """.query[(String, String, String, Option[java.time.Instant], String, Option[java.time.Instant])]
        .option
    }.map { case (rowKind, stateKind, payload, retryDeadline, fingerprints, expiresAt) =>
      StepRow(
        StepState.fromPersisted(rowKind, stateKind, payload, retryDeadline),
        InputFingerprints.of(fingerprints),
        expiresAt
      )
    }

  /** Escapes `%`, `_`, and the escape character itself so the caller's prefix is
    * matched literally against the key with a trailing `%` wildcard.
    */
  private def escapeLikePrefix(prefix: String): String =
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
      Option[ScopePath],
      java.time.Instant,
      Option[java.time.Instant],
      Int,
      Option[String]
    )

  private val InfoSelect: Fragment =
    fr"""
      SELECT workflow_instance_key, workflow_version_at_creation, generation,
      parent_workflow_id, parent_instance_key, parent_instance_scope, created_at,
      last_run_at, times_executed, terminal_state
      FROM workflow_instances
    """

  private def terminalStateEnumFromStr(state: String): WorkflowTerminalState = state match {
    case "completed"  => WorkflowTerminalState.Completed
    case "failed"     => WorkflowTerminalState.Failed
    case "cancelled"  => WorkflowTerminalState.Cancelled
    case "terminated" => WorkflowTerminalState.Terminated
    case other        => throw new StepSerializationFailed(s"Unknown terminal state '$other'")
  }

  private def toInfo(workflowId: WorkflowId, scope: ScopePath, r: InfoRow): WorkflowInstance.Info = {
    val (key, versionAtCreation, generation, parentWf, parentKey, parentScope, createdAt, lastRunAt, timesExecuted, terminalStateStr) = r
    val parentId = parentWf.map(pwf => WorkflowInstanceId(pwf, parentKey.getOrElse(""), parentScope.getOrElse(ScopePath.empty)))
    WorkflowInstance.Info(
      id = WorkflowInstanceId(workflowId, key, scope),
      parentId = parentId,
      generation = generation,
      terminalState = terminalStateStr.map(terminalStateEnumFromStr),
      workflowVersionAtCreation = versionAtCreation,
      createdAt = createdAt,
      lastRunAt = lastRunAt,
      timesExecuted = timesExecuted
    )
  }

  override def getWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Vector[WorkflowInstance.Info] = {
    val pattern = escapeLikePrefix(keyPrefix) + "%"
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
    }.map(toInfo(workflowId, ScopePath.empty, _))
  }

  override def deleteWorkflowInstancesByPrefix(
      workflowId: WorkflowId,
      keyPrefix: WorkflowInstanceKey,
      scope: ScopePath = ScopePath.empty
  ): Long = {
    val pattern = escapeLikePrefix(keyPrefix) + "%"
    runTransaction {
      for {
        _ <- sql"""
          DELETE FROM workflow_events
          WHERE workflow_id = $workflowId AND workflow_instance_scope = $scope AND workflow_instance_key LIKE $pattern ESCAPE '\'
        """.update.run
        
        deleted <- sql"""
          DELETE FROM workflow_instances
          WHERE workflow_id = $workflowId AND workflow_instance_scope = $scope AND workflow_instance_key LIKE $pattern ESCAPE '\'
        """.update.run
      } yield deleted.toLong
    }
  }

  override def getChildWorkflowInstances(parentId: WorkflowInstanceId): Vector[WorkflowInstance.Info] = {
    val rows = runTransaction {
      sql"""
        SELECT workflow_id, workflow_instance_scope, workflow_instance_key, workflow_version_at_creation, generation,
               parent_workflow_id, parent_instance_key, parent_instance_scope, created_at, last_run_at, times_executed, terminal_state
        FROM workflow_instances
        WHERE parent_workflow_id = ${parentId.workflowId}
          AND parent_instance_key = ${parentId.workflowInstanceKey}
          AND parent_instance_scope = ${parentId.scope}
        ORDER BY workflow_id, workflow_instance_key, workflow_instance_scope
      """.query[(WorkflowId, ScopePath, InfoRow)].to[Vector]
    }
    rows.map { case (wf, scope, r) => toInfo(wf, scope, r) }
  }

  // ---- Engine operations of the WorkflowRuntimeInternals SPI. Each uses this
  // ---- run's PostgresCurrentExecution handle carrying the run's fencing identity.

  override def writeStep(
      stepId: StepId,
      stepVersion: Long,
      row: StepRow
  )(using lease: Lease): Unit =
    runLeaseCheckedTransaction(lease) {
      for {
        _ <- upsertStepStateQ(lease, stepId, stepVersion, row)
        _ <- row.state.pendingRetryDeadline match {
          case Some(deadline) =>
            insertRetrySubscriptionQ(lease, stepId, stepVersion, deadline)

          case None =>
            deleteTimerSubscriptionsQ(lease, stepId, stepVersion)
        }
      } yield ()
    }

  override def deleteStep(stepId: StepId, stepVersion: Long)(using lease: Lease): Unit =
    runLeaseCheckedTransaction(lease) {
      for {
        _ <- sql"""
          DELETE FROM workflow_steps
          WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope} AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion
        """.update.run
        _ <- deleteSiteSubscriptionsQ(lease, stepId, stepVersion)
      } yield ()
    }

  override def readAwaitSignalCandidates(signalKey: SignalKey)(using lease: Lease): Vector[SubscriberMatch] =
    runTransaction {
      for {
        cursor <- sql"""
          SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
          WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope} AND signal_key = $signalKey
        """.query[Long].unique
        
        direct <- sql"""
          SELECT sequence_id, payload, created_at FROM workflow_events
          WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
            AND event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor
          ORDER BY sequence_id
        """.query[(Long, String, java.time.Instant)].to[Vector]
        
        inherited <- readInheritedCandidatesQ(lease, signalKey, cursor)
      } yield (direct ++ inherited).sortBy(_._1)
          .map { case (seq, payload, createdAt) =>
            SubscriberMatch("", seq, SerializedValue.of(payload), createdAt)
          }
    }

  override def evaluateWait(
      waitSite: WaitSite,
      subscribers: Vector[Subscriber]
  )(decide: Vector[SubscriberMatch] => Option[WaitResolution])(using lease: Lease): Option[SerializedValue] = {
    runLeaseCheckedTransaction(lease)(fireDueTimerSubscriptionsQ(lease, waitSite.stepId, waitSite.stepVersion))
    runLeaseCheckedTransaction(lease) {
      for {
        _ <- subscribers.traverse_(registerSubscriberQ(lease, waitSite, _))
        perSubscriber <- subscribers.traverse(subscriber => readSubscriberEventsQ(lease, waitSite, subscriber).map(subscriber -> _))
        candidates = perSubscriber.flatMap(_._2).sortBy(_.sequenceId)
        resolution = decide(candidates)
        result <- resolution match {
          case Some(r) =>
            for {
              _ <- upsertStepStateQ(
                     lease,
                     waitSite.stepId,
                     waitSite.stepVersion,
                     StepRow(
                       StepState.Await(r.payload),
                       waitSite.inputFingerprints,
                       waitSite.expiresAt
                     )
                   )
              _ <- r.advanceSignalCursor.traverse_ { case (k, seq) => advanceCursorQ(lease, k, seq) }
              _ <- deleteSiteSubscriptionsQ(lease, waitSite.stepId, waitSite.stepVersion)
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
              .traverse_ { case (k, seq) => advanceCursorQ(lease, k, seq) }
              .as(None)
        }
      } yield result
    }
  }

  override def deleteSubscriptionsUnderScopePaths(scopePaths: Seq[ScopePath])(using lease: Lease): Unit =
    runLeaseCheckedTransaction(lease)(deleteSubscriptionsUnderScopePathsQ(lease, scopePaths))

  override def deleteStepsUnderScopePaths(scopePaths: Seq[ScopePath])(using lease: Lease): Unit =
    runLeaseCheckedTransaction(lease) {
      scopePaths.traverse_ { path =>
        require(path.segments.nonEmpty, "a step scope path must never be empty; an empty prefix would match every row")
        sql"""
          DELETE FROM workflow_steps
          WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
            AND step_scope_within_instance[1:${path.segments.length}] = $path
        """.update.run.map(_ => ())
      }
    }

  override def requestChildCloseUnderScopePath(scopePathPrefix: ScopePath)(using lease: Lease): Unit =
    runLeaseCheckedTransaction(lease) {
      require(scopePathPrefix.segments.nonEmpty, "a step scope path must never be empty; an empty prefix would match every child")
      // Child instances store their creation scope as the parent's instance
      // scope, the parent's workflow id and `key@generation` marker, and then
      // the parent's step scope path; translate the prefix accordingly.
      val target = lease.instanceScope.segments ++
        Vector(lease.workflowId, s"${lease.instanceKey}@${lease.generation}") ++
        scopePathPrefix.segments
      
      insertChildCloseRequestQ(lease.workflowId, lease.instanceKey, lease.instanceScope, target)
    }

  override def isCancellationRequested(instanceId: WorkflowInstanceId): Boolean = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    runTransaction {
      sql"""
        SELECT cancel_requested_at FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
      """.query[Option[Instant]].unique
    }.isDefined
  }

  override def renewLease()(using lease: Lease): Unit = {
    val now = clock.instant()
    val expires = now.plus(java.time.Duration.ofNanos(leaseDuration.toNanos))
    val updated = runTransaction {
      sql"""
        UPDATE workflow_instances
        SET lease_expires_at = $expires
        WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
          AND lease_owner = ${lease.workerId} AND fencing_token = ${lease.fencingToken}
          AND terminal_state IS NULL
      """.update.run
    }
    if (updated != 1) throw LeaseLostException(lease.instanceId)
  }

  /** Runs `write` inside one transaction, guarded by an exclusive lock on the
    * instance row and a fencing check in one statement; throws
    * [[LeaseLostException]] if the lease no longer belongs to this run (or the
    * instance row is gone), affecting no rows. Callers that do not need the
    * result simply discard it.
    *
    * The lock and the fence share one `SELECT ... FOR UPDATE`: under READ
    * COMMITTED Postgres re-checks the `WHERE` clause against the latest row
    * version after acquiring the lock, so a row that no longer records this
    * run's owner and token matches no rows. Both `lease_owner` and
    * `fencing_token` are required: lease recovery clears the owner without
    * bumping the token.
    */
  private def runLeaseCheckedTransaction[A](lease: Lease)(write: ConnectionIO[A]): A = {
    val res: Option[A] = runTransaction {
      for {
        fenceOk <- sql"""
          SELECT 1 FROM workflow_instances
          WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
            AND lease_owner = ${lease.workerId} AND fencing_token = ${lease.fencingToken}
          FOR UPDATE
        """.query[Int].option
        
        r <- if (fenceOk.isDefined) write.map(Some(_)) else None.pure[ConnectionIO]
      } yield r
    }
    res match {
      case Some(a) => a
      case None    => throw LeaseLostException(lease.instanceId)
    }
  }

  private val RetrySubscriberKey: String = "__retry__"

  private def upsertStepStateQ(
      lease: Lease,
      stepId: StepId,
      stepVersion: Long,
      row: StepRow
  ): ConnectionIO[Unit] = {
    val now = clock.instant()
    val fingerprintText = row.inputFingerprints.raw
    val rowKind = row.state.rowKindText
    val (stateKind, payloadText, retryDeadline) = row.state.persistedColumns
    val stepRowFilter =
      fr"workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}" ++
        fr" AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion"
    for {
      // A step key belongs to exactly one step kind. The stored row's kind must
      // match the written row's kind; a mismatch is a definition error, not an
      // overwrite. The lease fencing serializes runs of one instance, so the
      // check and the write cannot race.
      existingKind <- (sql"SELECT step_kind FROM workflow_steps WHERE " ++ stepRowFilter).query[String].option
      
      _ <- existingKind match {
        case None =>
          sql"""
            INSERT INTO workflow_steps (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, step_kind, state_kind, state_payload, retry_deadline, input_fingerprints, expires_at, created_at, updated_at)
            VALUES (${lease.workflowId}, ${lease.instanceKey}, ${lease.instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $rowKind, $stateKind, $payloadText, $retryDeadline, $fingerprintText, ${row.expiresAt}, $now, $now)
          """.update.run

        case Some(`rowKind`) =>
          (sql"""
            UPDATE workflow_steps
            SET state_kind = $stateKind, state_payload = $payloadText, retry_deadline = $retryDeadline, input_fingerprints = $fingerprintText, expires_at = ${row.expiresAt}, updated_at = $now
            WHERE
          """ ++ stepRowFilter).update.run

        case Some(storedKind) =>
          throw new StepKeyConflictException(
            s"Step key '${stepId.key}' at version $stepVersion is already stored as a '$storedKind' row and cannot be rewritten as a '$rowKind' row; a step key belongs to exactly one step kind"
          )
      }
    } yield ()
  }

  private def insertRetrySubscriptionQ(
      lease: Lease,
      stepId: StepId,
      stepVersion: Long,
      deadline: java.time.Instant
  ): ConnectionIO[Unit] =
    for {
      _ <- sql"""
        DELETE FROM workflow_timer_subscriptions
        WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
          AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion AND subscriber_key = $RetrySubscriberKey
      """.update.run
      
      _ <- sql"""
        INSERT INTO workflow_timer_subscriptions
          (timer_id, workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, deadline)
        VALUES (gen_random_uuid(), ${lease.workflowId}, ${lease.instanceKey}, ${lease.instanceScope}, ${stepId.key}, ${stepId.scope}, $stepVersion, $RetrySubscriberKey, $deadline)
        ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, step_scope_within_instance) DO NOTHING
      """.update.run
    } yield ()

  private def fireDueTimerSubscriptionsQ(
      lease: Lease,
      stepId: StepId,
      stepVersion: Long
  ): ConnectionIO[Unit] = {
    val now = clock.instant()
    for {
      due <- sql"""
        SELECT timer_id FROM workflow_timer_subscriptions
        WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
          AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion
          AND deadline <= $now
        ORDER BY deadline, timer_id
        FOR UPDATE
      """.query[java.util.UUID].to[Vector]
      _ <- due.traverse_ { subId =>
        for {
          exists <- sql"""
            SELECT 1 FROM workflow_events
            WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
              AND event_kind = 'TimerFired' AND event_key = ${subId.toString}
          """.query[Int].option
          
          _ <- if (exists.isEmpty) 
              appendEvent(lease.workflowId, lease.instanceKey, lease.instanceScope, "TimerFired", subId.toString, "")
            else 
             ().pure[ConnectionIO]
        } yield ()
      }
    } yield ()
  }

  private def registerSubscriberQ(lease: Lease, waitSite: WaitSite, subscriber: Subscriber): ConnectionIO[Unit] =
    subscriber match {
      case Subscriber.Signal(subscriberKey, signalKey) =>
        sql"""
          INSERT INTO workflow_signal_subscriptions (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, signal_key)
          VALUES (${lease.workflowId}, ${lease.instanceKey}, ${lease.instanceScope}, ${waitSite.stepId.key}, ${waitSite.stepId.scope}, ${waitSite.stepVersion}, $subscriberKey, $signalKey)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, signal_key) DO NOTHING
        """.update.run.map(_ => ())
        
      case Subscriber.Timer(subscriberKey, deadline) =>
        sql"""
          INSERT INTO workflow_timer_subscriptions
            (timer_id, workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, deadline)
          VALUES (gen_random_uuid(), ${lease.workflowId}, ${lease.instanceKey}, ${lease.instanceScope}, ${waitSite.stepId.key}, ${waitSite.stepId.scope}, ${waitSite.stepVersion}, $subscriberKey, $deadline)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, step_scope_within_instance) DO NOTHING
        """.update.run.map(_ => ())
        
      case Subscriber.Completion(subscriberKey, completedWorkflowId, completedWorkflowInstanceKey, completedScope) =>
        sql"""
          INSERT INTO workflow_completion_subscriptions
            (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, completed_workflow_id, completed_workflow_instance_key, completed_workflow_instance_scope)
          VALUES (${lease.workflowId}, ${lease.instanceKey}, ${lease.instanceScope}, ${waitSite.stepId.key}, ${waitSite.stepId.scope}, ${waitSite.stepVersion}, $subscriberKey, $completedWorkflowId, $completedWorkflowInstanceKey, $completedScope)
          ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, completed_workflow_id, completed_workflow_instance_key, completed_workflow_instance_scope) DO NOTHING
        """.update.run.map(_ => ())
    }

  private def readSubscriberEventsQ(lease: Lease, waitSite: WaitSite, subscriber: Subscriber): ConnectionIO[Vector[SubscriberMatch]] =
    subscriber match {
      case Subscriber.Signal(subscriberKey, signalKey) =>
        for {
          cursor <- sql"""
            SELECT COALESCE(MAX(sequence_id), 0) FROM signal_cursor
            WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope} AND signal_key = $signalKey
          """.query[Long].unique
          
          direct <- sql"""
            SELECT sequence_id, payload, created_at FROM workflow_events
            WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
              AND event_kind = 'Signal' AND event_key = $signalKey AND sequence_id > $cursor
            ORDER BY sequence_id
          """.query[(Long, String, Instant)].to[Vector]
          
          inherited <- readInheritedCandidatesQ(lease, signalKey, cursor)
        } yield (direct ++ inherited).sortBy(_._1)
            .map { case (seq, payload, createdAt) =>
              SubscriberMatch(subscriberKey, seq, SerializedValue.of(payload), createdAt)
            }
        
      case Subscriber.Timer(subscriberKey, _) =>
        for {
          subId <- sql"""
            SELECT timer_id FROM workflow_timer_subscriptions
            WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
              AND step_id = ${waitSite.stepId.key} AND step_scope_within_instance = ${waitSite.stepId.scope} AND step_version = ${waitSite.stepVersion} AND subscriber_key = $subscriberKey
          """.query[UUID].option
          
          events <- subId match {
            case None => Vector.empty[SubscriberMatch].pure[ConnectionIO]

            case Some(id) =>
              sql"""
                SELECT sequence_id, created_at FROM workflow_events
                WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
                  AND event_kind = 'TimerFired' AND event_key = ${id.toString}
                ORDER BY sequence_id
              """.query[(Long, Instant)].to[Vector]
                .map(_.map { case (seq, createdAt) => 
                  SubscriberMatch(subscriberKey, seq, SerializedValue.of(""), createdAt) 
                })
          }
        } yield events
        
      case Subscriber.Completion(subscriberKey, completedWorkflowId, completedWorkflowInstanceKey, completedScope) =>
        sql"""
          SELECT sequence_id, payload, created_at FROM workflow_events
          WHERE workflow_id = $completedWorkflowId AND workflow_instance_key = $completedWorkflowInstanceKey AND workflow_instance_scope = $completedScope
            AND event_kind = 'WorkflowCompleted' AND event_key = ''
          ORDER BY sequence_id
        """.query[(Long, String, java.time.Instant)].to[Vector]
          .map(_.map { case (seq, payload, createdAt) => 
            SubscriberMatch(subscriberKey, seq, SerializedValue.of(payload), createdAt) 
          })
    }

  /** Deletes the instance's subscription rows from all three subscription
    * tables (`workflow_signal_subscriptions`, `workflow_timer_subscriptions`,
    * `workflow_completion_subscriptions`) matching the instance identity and the
    * optional `extra` predicate. Every bulk subscription cleanup funnels through
    * here so that a future fourth subscription kind cannot be forgotten by a
    * single cleanup path. The generated statement is
    * `DELETE FROM <table> s WHERE <instance predicate> AND <extra>`, so `extra`
    * may reference the subscription alias `s` (and its columns) in a join-style
    * predicate.
    */
  private def deleteAllSubscriptionsQ(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: ScopePath,
      extra: Fragment = Fragment.empty
  ): ConnectionIO[Unit] = {
    val instanceFilter =
      fr"workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope"
      
    val filter =
      if (extra == Fragment.empty) instanceFilter
      else instanceFilter ++ fr" AND " ++ extra
      
    Vector(
      "workflow_signal_subscriptions",
      "workflow_timer_subscriptions",
      "workflow_completion_subscriptions"
    ).traverse_ { table =>
      (Fragment.const(s"DELETE FROM $table s WHERE ") ++ filter).update.run
    }
  }

  private def deleteSiteSubscriptionsQ(lease: Lease, stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
    deleteAllSubscriptionsQ(
      lease.workflowId,
      lease.instanceKey,
      lease.instanceScope,
      fr"step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion"
    )

  /** Deletes the subscription rows whose `step_scope_within_instance` equals one of
    * `scopePaths` or lies beneath one (a longer array with one of `scopePaths` as
    * a prefix), across all three subscription tables. `scopePaths` are always
    * non-empty, so the array-prefix equality is a valid slice predicate.
    */
  private def deleteSubscriptionsUnderScopePathsQ(lease: Lease, scopePaths: Seq[ScopePath]): ConnectionIO[Unit] =
    scopePaths.traverse_ { path =>
      require(path.segments.nonEmpty, "a step scope path must never be empty; an empty prefix would match every row")
      deleteAllSubscriptionsQ(
        lease.workflowId,
        lease.instanceKey,
        lease.instanceScope,
        fr"step_scope_within_instance[1:${path.segments.length}] = $path"
      )
    }

  private def deleteTimerSubscriptionsQ(lease: Lease, stepId: StepId, stepVersion: Long): ConnectionIO[Unit] =
    sql"""
      DELETE FROM workflow_timer_subscriptions
      WHERE workflow_id = ${lease.workflowId} AND workflow_instance_key = ${lease.instanceKey} AND workflow_instance_scope = ${lease.instanceScope}
        AND step_id = ${stepId.key} AND step_scope_within_instance = ${stepId.scope} AND step_version = $stepVersion
    """.update.run.map(_ => ())

  private def advanceCursorQ(lease: Lease, signalKey: SignalKey, sequenceId: Long): ConnectionIO[Unit] =
    sql"""
      INSERT INTO signal_cursor (workflow_id, workflow_instance_key, workflow_instance_scope, signal_key, sequence_id)
      VALUES (${lease.workflowId}, ${lease.instanceKey}, ${lease.instanceScope}, $signalKey, $sequenceId)
      ON CONFLICT (workflow_id, workflow_instance_key, workflow_instance_scope, signal_key) DO UPDATE
      SET sequence_id = GREATEST(signal_cursor.sequence_id, EXCLUDED.sequence_id)
    """.update.run.map(_ => ())

  /** The inherited `Signal` candidates for `signalKey` on this instance: events
    * stored on any currently eligible ancestor, after the instance's own cursor
    * and (unless `inheritPastEvents`) after the inherited-events start sequence
    * id captured at creation. With `inheritPastEvents` the retained older
    * events remain visible while they are ahead of the instance's cursor. The
    * eligible sources come from the shared [[inheritedSignalSources]]
    * restricted to this run and key, so the read path, the dispatcher, and the
    * full scan agree on eligibility by construction.
    */
  private def readInheritedCandidatesQ(
      lease: Lease,
      signalKey: SignalKey,
      cursor: Long
  ): ConnectionIO[Vector[(Long, String, java.time.Instant)]] =
    (fr"""
      SELECT e.sequence_id, e.payload, e.created_at
      FROM (
    """ ++ inheritedSignalSources(runningInstanceCandidate(lease, signalKey)) ++
    fr"""
      ) a
      JOIN workflow_events e
        ON e.workflow_id = a.source_wf AND e.workflow_instance_key = a.source_key AND e.workflow_instance_scope = a.source_scope
       AND e.event_kind = 'Signal' AND e.event_key = a.signal_key
      WHERE e.sequence_id > $cursor
        AND (a.past OR a.start_seq IS NULL OR e.sequence_id > a.start_seq)
      ORDER BY e.sequence_id
    """).query[(Long, String, java.time.Instant)].to[Vector]

  /** The per-run execution handle of this runtime: the fencing identity of one
    * run (worker, fencing token, generation) plus the instance identity and the
    * instance row re-read under the lease. The runtime creates it when a run's
    * lease is acquired; core only threads it back to this runtime's engine
    * operations and reads [[currentWfInstance]] to build the run's context.
    */
  final class PostgresCurrentExecution(
      val workerId: String,
      val fencingToken: Long,
      val workflowId: WorkflowId,
      val instanceKey: WorkflowInstanceKey,
      val instanceScope: ScopePath,
      val generation: Long,
      private[atomicflow] val currentWfInstance: InstanceRecord
  ) extends LeaseHandle {
    private[PostgresWorkflowRuntime] val instanceId = WorkflowInstanceId(workflowId, instanceKey, instanceScope)
  }

  override def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit = {
    val now = clock.instant()
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
      sql"""
        SELECT terminal_state FROM workflow_instances
        WHERE workflow_id = ${instanceId.workflowId}
          AND workflow_instance_key = ${instanceId.workflowInstanceKey}
          AND workflow_instance_scope = ${instanceId.scope}
      """.query[Option[String]].unique
    }

  /** Whether the instance's lease is still owned by `worker` at `token` — i.e.
    * the same worker and fencing token still hold it. */
  private[atomicflow] def isLeaseStillOwned(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  ): Boolean =
    runTransaction {
      sql"""
        SELECT 1 FROM workflow_instances
        WHERE workflow_id = ${instanceId.workflowId} AND workflow_instance_key = ${instanceId.workflowInstanceKey} AND workflow_instance_scope = ${instanceId.scope}
          AND lease_owner = $worker AND fencing_token = $token
      """.query[Int].option
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
