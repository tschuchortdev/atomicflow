package atomicflow.impl.db

import atomicflow.*
import atomicflow.internal.{StoredStep, WorkflowExecution}
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
      leaseAcquireTimeout: FiniteDuration = 30.seconds
  )(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(ds, clock, leaseDuration, leaseAcquireTimeout)
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
    clock: Clock,
    leaseDuration: FiniteDuration = 5.minutes,
    leaseAcquireTimeout: FiniteDuration = 30.seconds
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

  private def runSync[A](fa: ConnectionIO[A]): A =
    fa.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)

  private def workerId: String = s"$processUuid:${Thread.currentThread().getId}"

  private def leaseExpiry(now: java.time.Instant): java.time.Instant =
    now.plus(java.time.Duration.ofNanos(leaseDuration.toNanos))

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

  override def runWorkflowInstance[In, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId
  )(using cacheableThrowable: Cacheable[Throwable]): WorkflowRunResult[Out] = {
    val workflowId = instanceId.workflowId
    val key = instanceId.workflowInstanceKey
    val scope = instanceId.scope
    val outCacheable = workflow.outputCacheable

    given Cacheable[Out] = outCacheable
    val completionCodec = summon[Cacheable[WorkflowCompletionResult[Out]]]

    val row = runSync {
      sql"""SELECT terminal_state, terminal_outcome, input, workflow_version_at_creation
            FROM workflow_instances
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope""".query[
          (Option[String], Option[String], String, Long)
        ].option
    }

    val (terminalState, terminalOutcome, inputSerialized, versionAtCreation) = row match {
      case Some(r) => r
      case None =>
        throw new WorkflowNotFoundException(s"Workflow instance not found: $instanceId")
    }

    terminalState match {
      case Some(state) =>
        return handleTerminalRead(state, terminalOutcome, outCacheable, completionCodec, instanceId)
      case None => ()
    }

    val worker = workerId
    val token = acquireLease(workflowId, key, scope, worker) match {
      case Some(t) => t
      case None    => throw LeaseUnavailableException(instanceId)
    }

    try {
      val now = clock.instant()
      val bumped = runSync {
        sql"""UPDATE workflow_instances
              SET times_executed = times_executed + 1, last_run_at = $now
              WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                AND lease_owner = $worker AND fencing_token = $token""".update.run
      }
      if (bumped != 1) throw LeaseLostException(instanceId)

      val input = workflow.inputCacheable.read(inputSerialized)
      val execution = new PostgresExecution(worker, token, workflowId, key, scope)
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

      var outValue: Out = null.asInstanceOf[Out]
      var suspended = false
      try {
        outValue = workflow.body(input)(using ctx)
      } catch {
        case _: WorkflowSuspendedException =>
          suspended = true
        case _: LeaseLostException => throw new LeaseLostException(s"Workflow instance lease lost during run: $instanceId")
        case WorkflowNonFatal(t) =>
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

  /** One conditional lease-acquire attempt; returns the new fencing token on
    * success, `None` if the lease is held by a live owner or the instance is
    * terminal. The acquire and the fencing-token read-back are a single
    * `RETURNING` statement.
    */
  private def tryAcquireOnce(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String
  ): Option[Long] = {
    val now = clock.instant()
    val expires = leaseExpiry(now)
    runSync {
      sql"""UPDATE workflow_instances
            SET lease_owner = $worker, fencing_token = fencing_token + 1, lease_expires_at = $expires
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND terminal_state IS NULL
              AND (lease_owner IS NULL OR lease_expires_at <= $now)
            RETURNING fencing_token""".query[Long].option
    }
  }

  /** Bounded poll of the conditional lease acquire, every 100ms up to
    * `leaseAcquireTimeout`.
    */
  private def acquireLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String
  ): Option[Long] = {
    val pollIntervalMillis = 100L
    val deadlineNanos = System.nanoTime() + leaseAcquireTimeout.toNanos
    var acquired: Option[Long] = None
    while (acquired.isEmpty && System.nanoTime() < deadlineNanos) {
      acquired = tryAcquireOnce(workflowId, key, scope, worker)
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

  /** The guarded terminal transition, atomic with the `WorkflowCompleted` event
    * append: the guarded `terminal_state`/`terminal_outcome` update and the event
    * append commit together in one transaction. Wins only if the instance is not
    * already terminal and the lease is still ours. Returns rows updated (1 if this
    * writer won the transition, 0 if another writer already made it terminal).
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
              SET terminal_state = $state, terminal_outcome = $payload
              WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
                AND terminal_state IS NULL AND lease_owner = $worker AND fencing_token = $token""".update.run
        _ <- if (updated == 1) appendCompletedEvent(workflowId, key, scope, payload)
             else ().pure[ConnectionIO]
      } yield updated
    }

  /** Appends a `WorkflowCompleted` event via the global event-append protocol:
    * `pg_advisory_xact_lock` + `nextval('workflow_event_sequence')` + insert, all
    * within the caller's transaction and held until commit.
    */
  private def appendCompletedEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      payload: String
  ): ConnectionIO[Unit] =
    for {
      _ <- takeEventAppendLock
      sequenceId <- sql"SELECT nextval('workflow_event_sequence')".query[Long].unique
      _ <- sql"""
        INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, key, scope, event_key, payload)
        VALUES ($sequenceId, 'WorkflowCompleted', $workflowId, $key, $scope, '', $payload)
      """.update.run
    } yield ()

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
    readStepRow(instanceId.workflowId, instanceId.workflowInstanceKey, instanceId.scope, stepId.key, stepVersion)

  private def readStepRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      stepId: String,
      stepVersion: Long
  ): Option[StoredStep] =
    runSync {
      sql"""SELECT state_kind, state_payload, input_fingerprints, expires_at FROM workflow_steps
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope AND step_id = $stepId AND step_version = $stepVersion""".query[
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

  private final class PostgresExecution(
      val workerId: String,
      val fencingToken: Long,
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      instanceScope: String
  ) extends WorkflowExecution {

    private val instanceId = WorkflowInstanceId(workflowId, key, instanceScope)

    override def currentScope: String = ""

    override def now: java.time.Instant = clock.instant()

    override def renewLease(): Unit = {
      val now = clock.instant()
      val expires = now.plus(java.time.Duration.ofNanos(leaseDuration.toNanos))
      val updated = runSync {
        sql"""UPDATE workflow_instances
              SET lease_expires_at = $expires
              WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                AND lease_owner = $workerId AND fencing_token = $fencingToken
                AND terminal_state IS NULL""".update.run
      }
      if (updated != 1) throw LeaseLostException(instanceId)
    }

    override def lookupStep(stepId: StepId, stepVersion: Long): Option[StoredStep] =
      readStepRow(workflowId, key, instanceScope, stepId.key, stepVersion)

    override def writeStepStarted(
        stepId: StepId,
        stepVersion: Long,
        stepKind: String,
        inputFingerprints: String
    ): Unit =
      fenced {
        val now = clock.instant()
        sql"""INSERT INTO workflow_steps (workflow_id, key, scope, step_id, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
              VALUES ($workflowId, $key, $instanceScope, ${stepId.key}, $stepVersion, $stepKind, 'started', '', $inputFingerprints, NULL, $now, $now)
              ON CONFLICT (workflow_id, key, scope, step_id, step_version) DO UPDATE
              SET state_kind = 'started', state_payload = '', input_fingerprints = EXCLUDED.input_fingerprints, expires_at = NULL, updated_at = $now""".update.run
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
        val now = clock.instant()
        sql"""INSERT INTO workflow_steps (workflow_id, key, scope, step_id, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
              VALUES ($workflowId, $key, $instanceScope, ${stepId.key}, $stepVersion, $stepKind, 'succeeded', $payload, $inputFingerprints, $expiresAt, $now, $now)
              ON CONFLICT (workflow_id, key, scope, step_id, step_version) DO UPDATE
              SET state_kind = 'succeeded', state_payload = EXCLUDED.state_payload, input_fingerprints = EXCLUDED.input_fingerprints, expires_at = EXCLUDED.expires_at, updated_at = $now""".update.run
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
        val now = clock.instant()
        sql"""INSERT INTO workflow_steps (workflow_id, key, scope, step_id, step_version, step_kind, state_kind, state_payload, input_fingerprints, expires_at, created_at, updated_at)
              VALUES ($workflowId, $key, $instanceScope, ${stepId.key}, $stepVersion, $stepKind, 'failed', $payload, $inputFingerprints, $expiresAt, $now, $now)
              ON CONFLICT (workflow_id, key, scope, step_id, step_version) DO UPDATE
              SET state_kind = 'failed', state_payload = EXCLUDED.state_payload, input_fingerprints = EXCLUDED.input_fingerprints, expires_at = EXCLUDED.expires_at, updated_at = $now""".update.run
      }

    override def deleteStep(stepId: StepId, stepVersion: Long): Unit =
      fenced {
        sql"""DELETE FROM workflow_steps
              WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope AND step_id = ${stepId.key} AND step_version = $stepVersion""".update.run
      }

    /** Runs `write` inside one transaction, guarded by an exclusive lock on the
      * instance row and a fencing check; throws [[LeaseLostException]] if the
      * lease no longer belongs to this run, affecting no rows.
      */
    private def fenced[A](write: ConnectionIO[A]): Unit = {
      val ok = runSync {
        for {
          _ <- sql"""SELECT 1 FROM workflow_instances
                     WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                     FOR UPDATE""".query[Int].unique
          fenceOk <- sql"""SELECT 1 FROM workflow_instances
                           WHERE workflow_id = $workflowId AND key = $key AND scope = $instanceScope
                             AND lease_owner = $workerId AND fencing_token = $fencingToken""".query[Int].option
          _ <- if (fenceOk.isDefined) write else ().pure[ConnectionIO]
        } yield fenceOk.isDefined
      }
      if (!ok) throw LeaseLostException(instanceId)
    }
  }

  private[atomicflow] override def upsertWakeup(instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit = {
    val now = clock.instant()
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
}
