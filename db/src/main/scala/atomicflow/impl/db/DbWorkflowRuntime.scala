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

import java.time.Instant
import java.util
import java.util.UUID
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.math.Ordered.orderingToOrdered

// TODO: rename to PostgresWorkflowRuntime
// TODO: either get rid of effect type completely or return it in methods
class DbWorkflowRuntime[F[_] : Async](xa: Transactor[F], dispatcher: Dispatcher[F]) extends WorkflowRuntime with WorkflowRuntime.GenerateIds {

  override def createWorkflowInstance[In, Out](
                                                workflowInstance: WorkflowInstanceBuilder[In, Out],
                                                in: In
                                              )(
                                                using Cacheable[In]
                                              ): Unit = {
    val id = workflowInstance.instanceId
    val workflowId = workflowInstance.workflow.meta.id
    val input = Cacheable[In].serialize(in).asInstanceOf[Array[Byte]]

    def simpleWorkflowCtx = workflowInstance.simpleWorkflowCtx

    runSync {
      // TODO: race condition. Use upsert with unique constraint on (id, input)?
      sql"SELECT input FROM workflow_instance WHERE id = $id"
        .query[Array[Byte]]
        .option
        .flatMap {
          case Some(prevInput) if util.Arrays.equals(prevInput, input) =>
            Monad[ConnectionIO].unit

          case Some(_) =>
            throw WorkflowInputConflictException()(using simpleWorkflowCtx)

          case None =>
            // TODO: may throw unique constraint violation here
            sql"INSERT INTO workflow_instance (id, workflow_id, input) VALUES ($id, $workflowId, $input)"
              .update
              .run
              .void
        }
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
    val id = workflowInstance.instanceId

    // TODO: What if workflow doesn't complete within 5 minutes? -> Bug: concurrent executions possible
    val lockDuration = java.time.Duration.ofMinutes(5)
    val now = Instant.now()

    def simpleWorkflowCtx = workflowInstance.simpleWorkflowCtx

    val serializedInput: Array[Byte] = runSync {
      sql"SELECT id, locked_until FROM workflow_instance where id = $id FOR UPDATE"
        .query[(WorkflowInstanceId, Option[Instant])]
        .option
        .flatMap {
          case None =>
            throw WorkflowNotFoundException()(using simpleWorkflowCtx)

          case Some((_, Some(lockedUntil))) if now.isBefore(lockedUntil) =>
            throw WorkflowLockedException()(using simpleWorkflowCtx)

          case Some((_, _)) =>
            val lockUntil = now.plus(lockDuration)
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE id = $id RETURNING input"
              .update
              .withUniqueGeneratedKeys[Array[Byte]]("input")
        }
    }

    val ctx = new atomicflow.WorkflowContext[In, Out] {
      override val meta: WorkflowMeta = workflowInstance.workflow.meta

      override val instanceId: WorkflowInstanceId = workflowInstance.instanceId

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
          ox.timeout(30.seconds) { // gives 30 seconds for the workflow to cancel itself before we are in bug territory
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE id = $id"
              .update
              .run
              .void
          }.catching { case _: TimeoutException =>
            throw RuntimeException("Could not renew lock on workflow instance in time")
          }
        }
      )
    } finally {
      val unlock =
        sql"""
        UPDATE workflow_instance
        SET locked_until = NULL
        WHERE id = $id
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
              workflow_instance_id = ${ctx.workflowCtx.instanceId} AND
              step_id = ${ctx.meta.id} AND
              step_version = ${ctx.meta.version} AND
              input_fingerprints = $inputFingerprints
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, step_version, input_fingerprints, is_only_once)
          VALUES ($id, $libraryVersion, ${ctx.workflowCtx.meta.id}, ${ctx.workflowCtx.instanceId}, ${ctx.meta.id}, ${ctx.meta.version}, $inputFingerprints, false)
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
              workflow_instance_id = ${ctx.workflowCtx.instanceId} AND
              step_id = ${ctx.meta.id} AND
              is_only_once = true AND
              is_overridden = false
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, is_only_once)
          VALUES ($id, $libraryVersion, ${ctx.workflowCtx.meta.id}, ${ctx.workflowCtx.instanceId}, ${ctx.meta.id}, true)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      val updateQuery: ConnectionIO[Unit] =
        sql"""
        UPDATE step_idempotency
        SET is_overridden = true
        WHERE workflow_id = ${ctx.workflowCtx.meta.id} AND
              workflow_instance_id = ${ctx.workflowCtx.instanceId} AND
              step_id = ${ctx.meta.id} AND
              is_only_once = true AND
              is_overridden = false
        """.update.run.void

      runSync {
        idQuery.flatMap {
          case Some(existing) =>
            stepIdempotencyIdOverrides.get(ctx.meta.id) match {
              case Some(overrideId) if overrideId != existing =>
                updateQuery >>
                  insertQuery(overrideId)
              case _ =>
                Monad[ConnectionIO].pure(existing)
            }
          case None =>
            val stepIdempotencyId = stepIdempotencyIdOverrides.getOrElse(
              ctx.meta.id,
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
        WHERE step_idempotency_id = ${stepIdempotencyId} and step_id = ${ctx.meta.id}
      """.query[(Array[Byte], Long, StepInputFingerprints)].option

      runSync(query).flatMap {
        case (data, version, fingerprints) if version == ctx.meta.version && fingerprints == inputFingerprints =>
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
        VALUES (${stepIdempotencyId}, ${ctx.meta.id}, ${ctx.meta.version}, $inputFingerprints, $data, $expiry)
        ON CONFLICT (step_idempotency_id) DO UPDATE
        SET step_version = ${ctx.meta.version},
            input_fingerprints = $inputFingerprints,
            output = $data,
            expiry = $expiry
      """.update.run.void

      runSync(query)
    }
  }

  private object DbSignalStore extends SignalStore {

    // TODO: check expiry
    private def select[A](signal: Signal[A])(using workflowCtx: SimpleWorkflowContext): ConnectionIO[Option[Array[Byte]]] =
      sql"""
      SELECT value FROM workflow_signals
      WHERE id=${signal.meta.id} AND workflow_id=${workflowCtx.meta.id} AND workflow_instance_id=${workflowCtx.instanceId}
      """.query[Array[Byte]].option

    override def getSignalValue[A](signal: Signal[A])(using workflowCtx: SimpleWorkflowContext): Option[A] =
      runSync {
        select(signal)
      }.map { bytes =>
        signal.asCacheable.deserialize(bytes.asInstanceOf[IArray[Byte]])
      }

    @throws[SignalConflictException]
    override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using workflowCtx: SimpleWorkflowContext): Unit = {
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
              INSERT INTO workflow_signals (id, workflow_id, workflow_instance_id, value, expiry)
              SELECT ${signal.meta.id}, ${workflowCtx.meta.id}, ${workflowCtx.instanceId}, $bytes, $expiry
              WHERE EXISTS (
                SELECT 1
                FROM workflow_instance
                WHERE id = ${workflowCtx.instanceId}
              )
              """.update.run.map {
              case 0 => throw WorkflowNotFoundException()
              case 1 => ()
            }
        }
      }
    }
  }

  private def runSync[A](fa: ConnectionIO[A]): A =
    dispatcher.unsafeRunSync(fa.transact(xa))

  @throws[SignalConflictException]
  override def setSignal[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using SimpleWorkflowContext): Unit =
    DbSignalStore.setSignalValue(signal, value, ttl)
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
    (for {
      // dispatcher cleanup primarily ensures that all dispatched IOs have finished running, so not doing the cleanup
      // is not that bad.
      dispatcher <- Dispatcher.parallel[IO].allocated.map(_._1)
      xa = Transactor.fromDataSource[IO](ds, awaitConnectionEc)
      _ <- IO(
        org.flywaydb.core.Flyway.configure
          .dataSource(ds)
      )
      flyway = Flyway.configure()
        .dataSource(ds)

      flywayInfo <- IO(flyway.load().info())
      _ <- IO(flyway
        .withBaselineMigrate(flywayInfo)
        .validateMigrationNaming(true)
        .load()
        .migrate())
    } yield
      new DbWorkflowRuntime[IO](xa, dispatcher))
      .unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)
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

  given Meta[WorkflowInstanceId] = Meta[UUID].imap(uuid =>
    WorkflowInstanceId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(WorkflowInstanceId.unwrap(id))
  )

  given Meta[SignalId] = Meta[UUID].imap(uuid =>
    SignalId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(SignalId.unwrap(id))
  )

  given Get[StepInputFingerprints] = pgDecoderGetT[StepInputFingerprints]

  given Put[StepInputFingerprints] = pgEncoderPutT[StepInputFingerprints]
}
