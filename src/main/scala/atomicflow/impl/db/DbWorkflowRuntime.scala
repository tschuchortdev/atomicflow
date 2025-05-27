package atomicflow.impl.db

import atomicflow.Constants.libraryVersion
import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints}
import atomicflow.{Cacheable, StepContext, StepIdempotencyId, StepInputConflictException, WorkflowInstanceBuilder, WorkflowInstanceId, WorkflowLockedException, WorkflowMeta, WorkflowRuntime}
import cats.effect.Async
import cats.effect.std.Dispatcher
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import atomicflow.*
import cats.syntax.all.*

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import DbWorkflowRuntime.given
import atomicflow.Fingerprintable.Fingerprinter
import cats.Monad
import doobie.postgres.circe.jsonb.implicits.*

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

    val insert =
      sql"""
      INSERT INTO workflow_instance (id, workflow_id, input)
      VALUES ($id, $workflowId, $input)
      ON CONFLICT (id) DO NOTHING
    """.update.run

    runSync(insert)
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

    val lock =
      sql"""
      UPDATE workflow_instance
      SET locked_until = $lockUntil
      WHERE id = $id AND (locked_until IS NULL OR locked_until < $now)
    """.update.run.map(_ > 0)

    val locked = runSync(lock)
    if (locked) throw new WorkflowLockedException()(using SimpleWorkflowContext(
      workflowMeta = workflowInstance.workflow.meta,
      workflowInstanceId = workflowInstance.instanceId
    ))

    val ctx = new atomicflow.WorkflowContext[In, Out] {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta

      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId

      override protected[atomicflow] def getFingerprinter: Fingerprinter =
        atomicflow.impl.Sha256Fingerprinter

      override protected[atomicflow] def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore =
        new DbStepIdempotencyStore()

      override protected[atomicflow] def getStepCache[StepOut: Cacheable](using StepContext[StepOut]): StepCache[StepOut] =
        new DbStepCache[StepOut]()
    }

    try {
      val inputBytes = runSync(loadInput(id)).get
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

  class DbStepIdempotencyStore(using ctx: StepContext[?]) extends StepIdempotencyStore {
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
              is_only_once = true
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, is_only_once)
          VALUES ($id, $libraryVersion, ${ctx.workflowCtx.meta.id}, ${ctx.workflowCtx.instanceId}, ${ctx.meta.id}, true)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      def updateQuery(id: StepIdempotencyId): ConnectionIO[StepIdempotencyId] =
        sql"""
        UPDATE step_idempotency
        SET id = ${id}
        WHERE workflow_id = ${ctx.workflowCtx.meta.id} AND
              workflow_instance_id = ${ctx.workflowCtx.instanceId} AND
              step_id = ${ctx.meta.id} AND
              is_only_once = true
        """.update.run.as(id)

      runSync {
        idQuery.flatMap {
          case Some(existing) =>
            /*overrideIdempotencyId match { TODO
              case Some(overrideId) if overrideId != existing =>
                updateQuery(overrideId)
              case _ =>*/
                Monad[ConnectionIO].pure(existing)
            //}
          case None =>
            val stepIdempotencyId = //overrideIdempotencyId.getOrElse {
              StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            //}
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
                      ttl: Option[FiniteDuration]
                    ): Unit = {
      val expiry = ttl.map(d => java.time.Instant.now().plusMillis(d.toMillis))
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
      """.update.run

      runSync(query)
    }
  }

  private def runSync[A](fa: ConnectionIO[A]): A =
    dispatcher.unsafeRunSync(fa.transact(xa))
}

object DbWorkflowRuntime {
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

  given Get[StepInputFingerprints] = pgDecoderGetT[StepInputFingerprints]

  given Put[StepInputFingerprints] = pgEncoderPutT[StepInputFingerprints]
}
