package atomicflow.impl.db

import atomicflow.*
import atomicflow.Constants.libraryVersion
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.db.DbWorkflowRuntime.given
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints, catching}
import cats.Monad
import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, Resource}
import cats.syntax.all.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import doobie.postgres.circe.jsonb.implicits.*
import doobie.postgres.implicits.*
import org.flywaydb.core.Flyway
import org.postgresql.util.PSQLException

import java.time.{Clock, Instant}
import java.util
import java.util.UUID
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.math.Ordered.orderingToOrdered

// TODO: rename to PostgresWorkflowRuntime
class DbWorkflowRuntime protected (ds: DataSource)(using awaitConnectionEc: ExecutionContext)
  extends WorkflowRuntime with WorkflowRuntime.DefaultGenerateIdsMixin {

  protected val xa = Transactor.fromDataSource[IO](ds, awaitConnectionEc)

  override def createWorkflowInstance[In, Out](
                                                workflow: Workflow[In, Out],
                                                in: In
                                              )(
                                                using cacheable: Cacheable[In],
                                                defaultSettings: WorkflowRunSettings,
                                                clk: Clock
                                              ): Unit = {
    val key = workflowInstance.instanceKey
    val workflowId = workflowInstance.workflow.meta.id
    val input = Cacheable[In].serialize(in).asInstanceOf[Array[Byte]]

    def simpleWorkflowCtx = workflowInstance.workflowInstanceMeta

    runSync {
      sql"""
        INSERT INTO workflow_instance (workflow_id, key, input) VALUES ($workflowId, $key, $input)
        ON CONFLICT (workflow_id, key) DO UPDATE
        SET input = workflow_instance.input -- idempotent update
        -- only allow update if input is unchanged --> we can check the number of updated rows to find out whether
        -- workflow_instance.input was different from EXCLUDED.input
        WHERE workflow_instance.input = EXCLUDED.input
          """
        .update
        .run
    } match {
      case 0 => throw WorkflowInputConflictException()(using simpleWorkflowCtx)
      case _ => ()
    }
  }

  override def runWorkflowInstance[In, Out](
                                             workflowInstance: WorkflowInstanceBuilder[In, Out],
                                             in: In
                                           )(
                                             using Cacheable[In]
                                           ): Out = {
    createWorkflowInstance(workflowInstance, in)
    recoverWorkflowInstance(workflowInstance)
  }

  override def recoverWorkflowInstance[In, Out](
                                                 workflowInstance: WorkflowInstanceBuilder[In, Out]
                                               )(
                                                 using Cacheable[In]
                                               ): Out = {
    val key = workflowInstance.instanceKey

    // TODO: What if workflow doesn't complete within 5 minutes? -> Bug: concurrent executions possible
    val lockDuration = java.time.Duration.ofMinutes(5)
    val now = Instant.now()

    def simpleWorkflowCtx = workflowInstance.workflowInstanceMeta

    val serializedInput: Array[Byte] = runSync {
      sql"SELECT key, locked_until FROM workflow_instance where key = $key FOR UPDATE"
        .query[(WorkflowInstanceKey, Option[Instant])]
        .option
        .flatMap {
          case None =>
            throw WorkflowNotFoundException()(using simpleWorkflowCtx)

          case Some((_, Some(lockedUntil))) if now.isBefore(lockedUntil) =>
            throw WorkflowLockedException()(using simpleWorkflowCtx)

          case Some((_, _)) =>
            val lockUntil = now.plus(lockDuration)
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE key = $key RETURNING input"
              .update
              .withUniqueGeneratedKeys[Array[Byte]]("input")
        }
    }

    val ctx = new atomicflow.WorkflowContext[In, Out] {
      override val meta: WorkflowMeta = workflowInstance.workflow.meta

      override val instanceKey: WorkflowInstanceKey = workflowInstance.instanceKey

      override protected[atomicflow] def getFingerprinter: Fingerprinter =
        atomicflow.impl.Sha256Fingerprinter

      override protected[atomicflow] def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore =
        new DbStepIdempotencyStore(workflowInstance.stepIdempotencyIdOverrides)

      override protected[atomicflow] def getStepCache[StepOut: Cacheable](using StepContext[StepOut]): StepCache[StepOut] =
        new DbStepCache[StepOut]

      override protected[atomicflow] def getSignalStore: SignalStore =
        DbSignalStore

      override protected[atomicflow] val defaultCacheTtl: FiniteDuration =
        workflowInstance.defaultCacheTtl
    }

    try {
      ox.raceResult(
        workflowInstance.workflow.body(ctx, Cacheable[In].deserialize(IArray.unsafeFromArray(serializedInput))),
        // background job refreshes the lock
        ox.forever {
          Thread.sleep(lockDuration.minus(java.time.Duration.ofMinutes(1)))
          val lockUntil = Instant.now().plus(lockDuration)
          ox.timeout(30.seconds) { // gives 1 minute minus 30 seconds for the workflow to cancel itself before we are in bug territory
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE key = $key"
              .update
              .run
              .void
          }.catching { case e: TimeoutException =>
            throw RuntimeException("Could not renew lock on workflow instance in time", e)
          }
        }
      )
    } finally {
      val unlock =
        sql"""
        UPDATE workflow_instance
        SET locked_until = NULL
        WHERE key = $key
      """.update.run
      runSync(unlock)
    }
  }

  private class DbStepIdempotencyStore(
                                stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                              )(
                                using ctx: StepContext[?]
                              ) extends StepIdempotencyStore {
    override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE library_version = $libraryVersion AND
              workflow_id = ${ctx.workflowCtx.meta.id} AND
              workflow_instance_key = ${ctx.workflowCtx.instanceKey} AND
              step_id = ${ctx.meta.stepId} AND
              step_version = ${ctx.meta.stepVersion} AND
              input_fingerprints = $inputFingerprints
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_key, step_id, step_version, input_fingerprints, is_only_once)
          VALUES ($id, $libraryVersion, ${ctx.workflowCtx.meta.id}, ${ctx.workflowCtx.instanceKey}, ${ctx.meta.stepId}, ${ctx.meta.stepVersion}, $inputFingerprints, false)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      runSync {
        idQuery.flatMap {
          case Some(existing) => Monad[ConnectionIO].pure(existing)
          case None =>
            val stepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            insertQuery(stepIdempotencyId)
        }
      }
    }

    override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE workflow_id = ${ctx.workflowCtx.meta.id} AND
              workflow_instance_key = ${ctx.workflowCtx.instanceKey} AND
              step_id = ${ctx.meta.stepId} AND
              is_only_once = true AND
              is_overridden = false
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_key, step_id, is_only_once)
          VALUES ($id, $libraryVersion, ${ctx.workflowCtx.meta.id}, ${ctx.workflowCtx.instanceKey}, ${ctx.meta.stepId}, true)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      val updateQuery: ConnectionIO[Unit] =
        sql"""
        UPDATE step_idempotency
        SET is_overridden = true
        WHERE workflow_id = ${ctx.workflowCtx.meta.id} AND
              workflow_instance_key = ${ctx.workflowCtx.instanceKey} AND
              step_id = ${ctx.meta.stepId} AND
              is_only_once = true AND
              is_overridden = false
        """.update.run.void

      runSync {
        idQuery.flatMap {
          case Some(existing) =>
            stepIdempotencyIdOverrides.get(ctx.meta.stepId) match {
              case Some(overrideId) if overrideId != existing =>
                updateQuery >>
                  insertQuery(overrideId)
              case _ =>
                Monad[ConnectionIO].pure(existing)
            }
          case None =>
            val stepIdempotencyId = stepIdempotencyIdOverrides.getOrElse(
              ctx.meta.stepId,
              StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            )
            insertQuery(stepIdempotencyId)
        }
      }
    }
  }

  private class DbStepCache[Out: Cacheable](using ctx: StepContext[Out]) extends StepCache[Out] {
    override def get(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints
                    ): Option[Out] = {
      val query = sql"""
        SELECT output, step_version, input_fingerprints FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId} and step_id = ${ctx.meta.stepId}
      """.query[(Array[Byte], Long, StepInputFingerprints)].option

      runSync(query).flatMap {
        case (data, version, fingerprints) if version == ctx.meta.stepVersion && fingerprints == inputFingerprints =>
          Some(Cacheable[Out].deserialize(data.asInstanceOf[IArray[Byte]]))
        case _ => throw new StepInputConflictException()
      }
    }

    override def put(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints,
                      value: Out,
                      ttl: FiniteDuration
                    ): Unit = {
      val expiry = java.time.Instant.now().plusMillis(ttl.toMillis)
      val data = Cacheable[Out].serialize(value).asInstanceOf[Array[Byte]]

      val query =
        sql"""
        INSERT INTO step_cache (step_idempotency_id, step_id, step_version, input_fingerprints, output, expiry)
        VALUES (${stepIdempotencyId}, ${ctx.meta.stepId}, ${ctx.meta.stepVersion}, $inputFingerprints, $data, $expiry)
        ON CONFLICT (step_idempotency_id) DO UPDATE
        SET step_version = ${ctx.meta.stepVersion},
            input_fingerprints = $inputFingerprints,
            output = $data,
            expiry = $expiry
      """.update.run.void

      runSync(query)
    }
  }

  private object DbSignalStore extends SignalStore {

    // TODO: check expiry
    private def select[A](signal: Signal[A])(using workflowCtx: WorkflowInstanceMeta): ConnectionIO[Option[Array[Byte]]] =
      sql"""
      SELECT value FROM workflow_signals
      WHERE id=${signal.meta.id} AND workflow_id=${workflowCtx.meta.id} AND workflow_instance_key=${workflowCtx.workflowInstanceKey}
      """.query[Array[Byte]].option

    override def getSignalValue[A](signal: Signal[A])(using workflowCtx: WorkflowInstanceMeta): Option[A] =
      runSync {
        select(signal)
      }.map { bytes => signal.asCacheable.deserialize(IArray.unsafeFromArray(bytes)) }

    @throws[SignalConflictException]
    override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using workflowCtx: WorkflowInstanceMeta): Unit = {
      val expiry = java.time.Instant.now().plusMillis(ttl.toMillis)
      val bytes: Array[Byte] = signal.asCacheable.serialize(value).asInstanceOf[Array[Byte]]

      runSync {
        select(signal).flatMap {
          case Some(prevBytes) if util.Arrays.equals(prevBytes, bytes) =>
            Monad[ConnectionIO].unit

          case Some(_) =>
            throw SignalConflictException(signal)

          case None =>
            sql"""
              INSERT INTO workflow_signals (id, workflow_id, workflow_instance_key, value, expiry)
              SELECT ${signal.meta.id}, ${workflowCtx.meta.id}, ${workflowCtx.workflowInstanceKey}, $bytes, $expiry
              WHERE EXISTS (
                SELECT 1
                FROM workflow_instance AS wi
                WHERE wi.workflow_id = ${workflowCtx.meta.id}
                  AND wi.key = ${workflowCtx.workflowInstanceKey}
              )
              """.update.run
              .recover {
                // 23505 is unique violation, i.e. duplicate primary key
                case e: PSQLException if e.getSQLState == "23505" => throw SignalConflictException(signal)
              }
              .map {
                case 0 => throw WorkflowNotFoundException()
                case 1 => ()
              }
        }
      }
    }
  }

  override def scheduleWakeupOnTimer(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, wakeupTime: Instant): Unit = {
    val now = Instant.now(clk)
    runSync {
      sql"""
           INSERT INTO workflows_awaiting_timer (awaiter_id, workflow_id, workflow_instance_key, restart_after, created_at)
           VALUES ($???, $workflowId, $workflowInstanceKey, $wakeupTime, $now)
           """
        .recover {
          // 23503 is foreign key constraint violation
          case e: PSQLException if e.getSQLState == "23503" && e.getMessage.contains("not present in table \"workflow_instance\"") =>
            throw WorkflowNotFoundException(e)
        }
    }
  }

  override def scheduleWakeupOnSignal(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, signal: Signal[_]): Unit = {
    val now = Instant.now(clk)
    runSync {
      sql"""
           INSERT INTO workflows_awaiting_signal (awaiter_id, workflow_id, workflow_instance_key, signal_id, created_at)
           VALUES ($???, $workflowId, $workflowInstanceKey, ${signal.meta.id}, $now)
           """
        .recover {
          // 23503 is foreign key constraint violation
          case e: PSQLException if e.getSQLState == "23503" && e.getMessage.contains("not present in table \"workflow_instance\"") =>
            throw WorkflowNotFoundException(e)
        }
    }
  }

  override def scheduleWakeupOnWorkflowCompletion(workflowInstanceKey: WorkflowInstanceKey, awaitedWorkflow: WorkflowInstanceKey): Unit = {
    val now = Instant.now(clk)
    runSync {
      sql"""
           INSERT INTO workflows_awaiting_signal (
              awaiter_id, workflow_id, workflow_instance_key,
              awaited_workflow_id, awaited_workflow_instance_key, created_at
            )
           VALUES ($???, $workflowId, $workflowInstanceKey, $??? $???, $now)
           """
        .recover {
          // 23503 is foreign key constraint violation
          case e: PSQLException if e.getSQLState == "23503" && e.getMessage.contains("not present in table \"workflow_instance\"") =>
            throw WorkflowNotFoundException(e)
        }
    }
  }

  @throws[SignalConflictException]
  override def setSignal[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using WorkflowInstanceMeta): Unit =
    DbSignalStore.setSignalValue(signal, value, ttl)

  private def runSync[A](fa: ConnectionIO[A]): A = {
    fa.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)
  }
}

object DbWorkflowRuntime {
  /**
   * Creates a new [[DbWorkflowRuntime]] including all necessary schemas in the database.
   * Since this function executes DDL statements on the database, it should not be called often; Reuse
   * [[DbWorkflowRuntime]] instances instead of creating new ones where possible.
   *
   * @param ds DataSource to be used to connect to the database
   * @param awaitConnectionEc Execution context to be used while getting a new connection from the data source.
   *                          Asking for a connection from the data source can block when the connection is established
   *                          for the first time or when the connection pool is exhausted. Therefore, this execution
   *                          context should be from an IO thread pool.
   */
  def apply(ds: DataSource)(using awaitConnectionEc: ExecutionContext): WorkflowRuntime = {
    import de.lhns.doobie.flyway.BaselineMigrations.BaselineMigrationOps
      val flyway = Flyway.configure().dataSource(ds)
      val flywayInfo  = flyway.load().info()
      flyway
        .withBaselineMigrate(flywayInfo)
        .validateMigrationNaming(true)
        .load()
        .migrate()

      new DbWorkflowRuntime(ds)(using awaitConnectionEc)
  }

  given Meta[StepIdempotencyId] = Meta[UUID].imap(uuid =>
    StepIdempotencyId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(StepIdempotencyId.unwrap(id))
  )

  given Meta[StepId] = Meta[UUID].imap(uuid =>
    StepId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(StepId.unwrap(id))
  )

  given Meta[WorkflowId] = Meta[UUID].imap(uuid =>
    WorkflowId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(WorkflowId.unwrap(id))
  )

  given Meta[SignalId] = Meta[UUID].imap(uuid =>
    SignalId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(SignalId.unwrap(id))
  )

  given Get[StepInputFingerprints] = pgDecoderGetT[StepInputFingerprints]

  given Put[StepInputFingerprints] = pgEncoderPutT[StepInputFingerprints]
}
