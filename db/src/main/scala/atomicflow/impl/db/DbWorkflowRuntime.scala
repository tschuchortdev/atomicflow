package atomicflow.impl.db

import atomicflow.*
import atomicflow.Constants.libraryVersion
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.db.DbWorkflowRuntime.given
import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints, SignalStore}
import cats.Monad
import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, Resource}
import cats.syntax.all.*
import de.lhns.doobie.flyway.BaselineMigrations.*
import de.lhns.doobie.flyway.Flyway
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import doobie.postgres.circe.jsonb.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import java.util
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

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
      sql"SELECT input FROM workflow_instance WHERE id = $id"
        .query[Array[Byte]]
        .option
        .flatMap {
          case Some(prevInput) if util.Arrays.equals(prevInput, input) =>
            Monad[ConnectionIO].unit

          case Some(_) =>
            throw WorkflowInputConflictException()(using simpleWorkflowCtx)

          case None =>
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

    val lockDuration = java.time.Duration.ofMinutes(5)
    val now = Instant.now()
    val lockUntil = now.plus(lockDuration)

    def simpleWorkflowCtx = workflowInstance.simpleWorkflowCtx

    runSync {
      sql"SELECT id, locked_until FROM workflow_instance where id = $id"
        .query[(WorkflowInstanceId, Option[Instant])]
        .option
        .flatMap {
          case None =>
            throw WorkflowNotFoundException()(using simpleWorkflowCtx)

          case Some((_, Some(lockedUntil))) if now.isBefore(lockedUntil) =>
            throw WorkflowLockedException()(using simpleWorkflowCtx)

          case Some((_, _)) =>
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE id = $id"
              .update
              .run
              .void
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
      val inputBytes = runSync(loadInput(id)).get /*TODO: .getOrElse {
        throw WorkflowNotFoundException()(using ctx)
      }*/
      workflowInstance.workflow.body(ctx, Cacheable[In].deserialize(inputBytes.asInstanceOf[IArray[Byte]]))
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

  private def loadInput(id: WorkflowInstanceId): ConnectionIO[Option[Array[Byte]]] =
    sql"SELECT input FROM workflow_instance WHERE id = $id".query[Array[Byte]].option

  class DbStepIdempotencyStore(
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

  class DbStepCache[Out: Cacheable](using ctx: StepContext[Out]) extends StepCache[Out] {
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

  object DbSignalStore extends SignalStore {

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
        signal.cacheable.deserialize(bytes.asInstanceOf[IArray[Byte]])
      }

    @throws[SignalConflictException]
    override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using workflowCtx: SimpleWorkflowContext): Unit = {
      val expiry = java.time.Instant.now().plusMillis(ttl.toMillis)
      val bytes: Array[Byte] = signal.cacheable.serialize(value).asInstanceOf[Array[Byte]]

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
  case class DbConfig(
                       driver: Option[String],
                       url: String,
                       user: String,
                       password: String,
                       poolSize: Option[Int]
                     ) {
    def driverOrDefault: String = driver.getOrElse("org.postgresql.Driver")

    def poolSizeOrDefault: Int = poolSize.getOrElse(32)
  }

  private def transactor(config: DbConfig): Resource[IO, Transactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](config.poolSizeOrDefault)
      xa <- HikariTransactor
        .newHikariTransactor[IO](
          config.driverOrDefault,
          config.url,
          config.user,
          config.password,
          ce
        )
      _ <- Flyway(xa) { flyway =>
        for {
          info <- flyway.info()
          _ <- flyway
            .configure(_
              .withBaselineMigrate(info)
              .validateMigrationNaming(true)
            )
            .migrate()
        } yield ()
      }
    } yield xa

  def apply(config: DbConfig): WorkflowRuntime = {
    (for {
      dispatcher <- Dispatcher.parallel[IO]
      xa <- transactor(config)
    } yield
      new DbWorkflowRuntime[IO](xa, dispatcher))
      .allocated.map(_._1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
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
