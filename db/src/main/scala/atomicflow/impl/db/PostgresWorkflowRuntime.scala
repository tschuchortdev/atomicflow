package atomicflow.impl.db

import atomicflow.*
import atomicflow.internal.WorkflowExecution
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

    // (a) load the row
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

    // (b) terminal instances return/throw their stored outcome without executing
    terminalState match {
      case Some(state) =>
        return handleTerminalRead(state, terminalOutcome, outCacheable, completionCodec, instanceId)
      case None => ()
    }

    // (c) acquire the execution lease (bounded poll; never steals a live lease)
    val worker = workerId
    val token = acquireLease(workflowId, key, scope, worker, instanceId) match {
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
      val execution = new PostgresExecution(worker, token)
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

      // (d)-(f) execute the body; classify outcome by what the body does
      var outValue: Out = null.asInstanceOf[Out]
      var suspended = false
      try {
        outValue = workflow.body(input)(using ctx)
      } catch {
        case _: WorkflowSuspendedException =>
          suspended = true
        case WorkflowNonFatal(t) =>
          // (e) body failure: terminal 'failed' + event; rethrow the decoded failure
          val payload = completionCodec.write(WorkflowCompletionResult.Failed(t))
          val updated = guardedTerminalTransition(workflowId, key, scope, worker, token, "failed", payload)
          if (updated == 1) appendCompletedEvent(workflowId, key, scope, payload)
          log.info(s"Workflow instance $instanceId failed", t)
          throw cacheableThrowable.read(cacheableThrowable.write(t))
      }

      if (suspended) {
        // (f) suspension: no terminal transition
        log.debug(s"Workflow instance $instanceId suspended")
        WorkflowRunResult.WorkflowSuspended
      } else {
        // (d) normal return: terminal 'completed' + event
        val payload = completionCodec.write(WorkflowCompletionResult.Completed(outValue))
        val updated = guardedTerminalTransition(workflowId, key, scope, worker, token, "completed", payload)
        if (updated == 1) {
          appendCompletedEvent(workflowId, key, scope, payload)
          log.debug(s"Workflow instance $instanceId completed")
          WorkflowRunResult.Result(outCacheable.read(outCacheable.write(outValue)))
        } else {
          // lost the terminal race; adopt the winner's stored outcome
          readTerminalAndReturn(workflowId, key, scope, outCacheable, completionCodec, instanceId)
        }
      }
    } finally {
      releaseLease(workflowId, key, scope, worker, token)
    }
  }

  /** One conditional lease-acquire attempt; returns the new fencing token on
    * success, `None` if the lease is held by a live owner or the instance is
    * terminal.
    */
  private def tryAcquireOnce(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String
  ): Option[Long] = {
    val now = clock.instant()
    val expires = leaseExpiry(now)
    val updated = runSync {
      sql"""UPDATE workflow_instances
            SET lease_owner = $worker, fencing_token = fencing_token + 1, lease_expires_at = $expires
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND terminal_state IS NULL
              AND (lease_owner IS NULL OR lease_expires_at <= $now)""".update.run
    }
    if (updated == 1)
      Some(
        runSync {
          sql"SELECT fencing_token FROM workflow_instances WHERE workflow_id = $workflowId AND key = $key AND scope = $scope"
            .query[Long]
            .unique
        }
      )
    else None
  }

  /** Bounded poll of the conditional lease acquire, every 100ms up to
    * `leaseAcquireTimeout`.
    */
  private def acquireLease(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      instanceId: WorkflowInstanceId
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

  /** The guarded terminal transition: writes `terminal_state`/`terminal_outcome`
    * atomically with the `WorkflowCompleted` event, winning only if the instance
    * is not already terminal and the lease is still ours. Returns rows updated.
    */
  private def guardedTerminalTransition(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      worker: String,
      token: Long,
      state: String,
      payload: String
  ): Int =
    runSync {
      sql"""UPDATE workflow_instances
            SET terminal_state = $state, terminal_outcome = $payload
            WHERE workflow_id = $workflowId AND key = $key AND scope = $scope
              AND terminal_state IS NULL AND lease_owner = $worker AND fencing_token = $token""".update.run
    }

  /** Appends a `WorkflowCompleted` event using the global event-append protocol:
    * `pg_advisory_xact_lock` + `nextval('workflow_event_sequence')`, held until
    * commit.
    */
  private def appendCompletedEvent(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      payload: String
  ): Unit =
    runSync {
      for {
        _ <- takeEventAppendLock
        sequenceId <- sql"SELECT nextval('workflow_event_sequence')".query[Long].unique
        _ <- sql"""
          INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, key, scope, event_key, payload)
          VALUES ($sequenceId, 'WorkflowCompleted', $workflowId, $key, $scope, '', $payload)
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

  private final class PostgresExecution(val workerId: String, val fencingToken: Long) extends WorkflowExecution

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
