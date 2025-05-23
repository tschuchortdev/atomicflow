package atomicflow.impl.db

import atomicflow.internal.{StepCache, StepContext, StepIdempotencyStore, StepInputConflictException, StepInputFingerprints}
import atomicflow.{StepContext, StepIdempotencyId, WorkflowInstanceBuilder, WorkflowInstanceId, WorkflowMeta, WorkflowRuntime}
import cats.effect.Async
import cats.effect.std.Dispatcher
import doobie.*
import doobie.implicits.*

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

class DbWorkflowRuntime[F[_]: Async](xa: Transactor[F], dispatcher: Dispatcher[F]) extends WorkflowRuntime with WorkflowRuntime.GenerateIds {

  override def createWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Unit = {
    val id = workflowInstance.instanceId
    val workflowId = workflowInstance.workflow.meta.id
    val input = serialize(in)

    val insert = sql"""
      INSERT INTO workflow_instance (id, workflow_id, input)
      VALUES ($id, $workflowId, $input)
      ON CONFLICT (id) DO NOTHING
    """.update.run

    runSync(insert)
  }

  override def runWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out], in: In): Out = {
    createWorkflowInstance(workflowInstance, in)
    recoverWorkflowInstance(workflowInstance)
  }

  override def recoverWorkflowInstance[In, Out](workflowInstance: WorkflowInstanceBuilder[In, Out]): Out = {
    val id = workflowInstance.instanceId

    val lockDuration = java.time.Duration.ofMinutes(5)
    val now = Instant.now()
    val lockUntil = now.plus(lockDuration)

    val lock = sql"""
      UPDATE workflow_instance
      SET locked_until = $lockUntil
      WHERE id = $id AND (locked_until IS NULL OR locked_until < $now)
    """.update.run

    val locked = runSync(lock)
    if (locked == 0) throw new Exception("WorkflowLockedException")

    val ctx = new atomicflow.WorkflowContext[In, Out] {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta
      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId
      override protected[atomicflow] def getFingerprinter = atomicflow.impl.Sha256Fingerprinter
      override protected[atomicflow] def getStepIdempotencyStore(using StepContext[?]): StepIdempotencyStore = new DbStepIdempotencyStore(summon[StepContext[?]])
      override protected[atomicflow] def getStepCache[StepOut](using StepContext[StepOut]): StepCache[StepOut] = new DbStepCache[StepOut](summon[StepContext[StepOut]])
    }

    try workflowInstance.workflow.body(ctx, runSync(loadInput(id)).get)
    finally {
      val unlock = sql"""
        UPDATE workflow_instance
        SET locked_until = NULL
        WHERE id = $id
      """.update.run
      runSync(unlock)
    }
  }

  private def loadInput(id: WorkflowInstanceId): ConnectionIO[Option[Array[Byte]]] =
    sql"SELECT input FROM workflow_instance WHERE id = $id".query[Array[Byte]].option

  class DbStepIdempotencyStore(ctx: StepContext[?]) extends StepIdempotencyStore {
    override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE library_version = ${ctx.meta.libraryVersion} AND
              workflow_id = ${ctx.meta.workflowId} AND
              workflow_instance_id = ${ctx.workflowInstanceId} AND
              step_id = ${ctx.meta.id} AND
              step_version = ${ctx.meta.version} AND
              input_fingerprints = $inputFingerprints
      """.query[UUID].option

      val insertQuery = for {
        id <- Async[F].delay(UUID.randomUUID())
        _ <- sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, step_version, input_fingerprints)
          VALUES ($id, ${ctx.meta.libraryVersion}, ${ctx.meta.workflowId}, ${ctx.workflowInstanceId}, ${ctx.meta.id}, ${ctx.meta.version}, $inputFingerprints)
          ON CONFLICT DO NOTHING
        """.update.run
      } yield StepIdempotencyId(id)

      runSync {
        idQuery.flatMap {
          case Some(existing) => Async[F].pure(StepIdempotencyId(existing))
          case None           => insertQuery
        }
      }
    }

    override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE workflow_id = ${ctx.meta.workflowId} AND
              workflow_instance_id = ${ctx.workflowInstanceId} AND
              step_id = ${ctx.meta.id} AND
              is_only_once = true
      """.query[UUID].option

      val insertQuery = for {
        id <- Async[F].delay(UUID.randomUUID())
        _ <- sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, is_only_once)
          VALUES ($id, ${ctx.meta.libraryVersion}, ${ctx.meta.workflowId}, ${ctx.workflowInstanceId}, ${ctx.meta.id}, true)
          ON CONFLICT DO NOTHING
        """.update.run
      } yield StepIdempotencyId(id)

      runSync {
        idQuery.flatMap {
          case Some(existing) => Async[F].pure(StepIdempotencyId(existing))
          case None           => insertQuery
        }
      }
    }

    override def overrideOnlyOnceStepIdempotencyId(stepIdempotencyId: StepIdempotencyId): Unit = {
      val updateQuery = sql"""
        UPDATE step_idempotency
        SET id = ${stepIdempotencyId.value}
        WHERE workflow_id = ${ctx.meta.workflowId} AND
              workflow_instance_id = ${ctx.workflowInstanceId} AND
              step_id = ${ctx.meta.id} AND
              is_only_once = true
      """.update.run

      runSync(updateQuery)
    }
  }

  class DbStepCache[Out](ctx: StepContext[Out]) extends StepCache[Out] {
    override def get(stepIdempotencyId: StepIdempotencyId, inputFingerprints: StepInputFingerprints): Option[Out] = {
      val query = sql"""
        SELECT output, step_version, input_fingerprints FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId.value}
      """.query[(Array[Byte], Long, String)].option

      runSync(query).flatMap {
        case (data, version, fingerprints) if version == ctx.meta.version && fingerprints == inputFingerprints =>
          Some(deserialize[Out](data))
        case _ => throw new StepInputConflictException()
      }
    }

    override def put(stepIdempotencyId: StepIdempotencyId, inputFingerprints: StepInputFingerprints, value: Out, ttl: Option[FiniteDuration]): Unit = {
      val expiry = ttl.map(d => java.time.Instant.now().plusMillis(d.toMillis))
      val data = serialize(value)

      val query = sql"""
        INSERT INTO step_cache (step_idempotency_id, step_id, step_version, input_fingerprints, output, expiry)
        VALUES (${stepIdempotencyId.value}, ${ctx.meta.id}, ${ctx.meta.version}, $inputFingerprints, $data, $expiry)
        ON CONFLICT (step_idempotency_id) DO UPDATE
        SET output = EXCLUDED.output,
            step_version = EXCLUDED.step_version,
            input_fingerprints = EXCLUDED.input_fingerprints,
            expiry = EXCLUDED.expiry
      """.update.run

      runSync(query)
    }
  }

  private def runSync[A](fa: ConnectionIO[A]): A =
    dispatcher.unsafeRunSync(fa.transact(xa))

  private def serialize[A](value: A): Array[Byte] = ??? // implement serialization
  private def deserialize[A](bytes: Array[Byte]): A = ??? // implement deserialization
}
