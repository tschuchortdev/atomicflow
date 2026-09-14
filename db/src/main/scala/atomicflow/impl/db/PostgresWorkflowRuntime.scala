package atomicflow.impl.db

import atomicflow.*
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.flywaydb.core.Flyway

import java.time.{Clock, Instant}
import javax.sql.DataSource
import scala.concurrent.ExecutionContext

object PostgresWorkflowRuntime {
  def apply(ds: DataSource)(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(ds, Clock.systemUTC())

  def apply(ds: DataSource, clock: Clock)(using ExecutionContext): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(ds, clock)
}

/** PostgreSQL backend for [[WorkflowRuntime]]. Applies the Flyway migrations on
  * construction, then serves create/instance operations against the shared
  * tables.
  */
class PostgresWorkflowRuntime private[atomicflow] (ds: DataSource, clock: Clock)(using ec: ExecutionContext)
    extends WorkflowRuntime {

  Flyway.configure().dataSource(ds).cleanDisabled(false).load().migrate()

  private val xa = Transactor.fromDataSource[IO](ds, ec)

  private def runSync[A](fa: ConnectionIO[A]): A =
    fa.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)

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
  )(using Cacheable[In], Cacheable[Out], Cacheable[Throwable]): WorkflowRunResult[Out] =
    throw new NotImplementedError("runWorkflowInstance is implemented in Task 2.2")

  private[atomicflow] override def upsertWakeup(instanceId: WorkflowInstanceId, scheduledAt: Instant): Unit = {
    val now = clock.instant()
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
