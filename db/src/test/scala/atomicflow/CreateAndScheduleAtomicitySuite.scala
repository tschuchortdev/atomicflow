package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.FaultInjector
import atomicflow.impl.db.PostgresWorkflowRuntime
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Clock
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** Atomicity of the Postgres create-and-schedule transaction: every injected
  * failure inside the transaction must leave neither the instance row nor the
  * wakeup row, and a crash after the commit must leave both.
  */
class CreateAndScheduleAtomicitySuite extends PostgresWorkflowRuntimeSuite {

  private final class InjectedFault(hook: String)
      extends RuntimeException(s"injected fault after $hook")

  private final class FailingInjector(failingHook: String) extends FaultInjector {
    private def inject(hook: String): Unit =
      if hook == failingHook then throw InjectedFault(hook)
    override def afterCreateInsert(): Unit = inject("afterCreateInsert")
    override def afterWakeupInsert(): Unit = inject("afterWakeupInsert")
    override def afterCreateAndScheduleCommit(): Unit = inject("afterCreateAndScheduleCommit")
  }

  private def runtimeWith(injector: FaultInjector): PostgresWorkflowRuntime =
    new PostgresWorkflowRuntime(dataSourceForTests, Clock.systemUTC(), faultInjector = injector)(using
      ExecutionContext.global
    )

  private def cleanRuntime: PostgresWorkflowRuntime = runtimeWith(FaultInjector.Noop)

  private def workflow(id: WorkflowId): Workflow[String, String] =
    Workflow[String, String](id = id)(body = in => in)

  private def instanceRows(workflowId: WorkflowId, key: WorkflowInstanceKey): Long =
    run(
      sql"SELECT COUNT(*) FROM workflow_instances WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'"
        .query[Long].unique
    )

  private def wakeupRows(workflowId: WorkflowId, key: WorkflowInstanceKey): Long =
    run(
      sql"SELECT COUNT(*) FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'"
        .query[Long].unique
    )

  private def terminalState(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[String] =
    run(
      sql"SELECT terminal_state FROM workflow_instances WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'"
        .query[Option[String]].unique
    )

  private def runScheduledToCompletion(
      rt: PostgresWorkflowRuntime,
      wf: Workflow[String, String],
      key: WorkflowInstanceKey
  ): Option[String] = {
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try runner.runDriverCycle()
    finally runner.stop(1.second)
    terminalState(wf.id, key)
  }

  /** Whether `t`'s cause chain contains the locally defined `InjectedFault`. */
  private def causeChainContains(t: Throwable): Boolean = {
    @annotation.tailrec
    def loop(current: Throwable): Boolean =
      current match {
        case null             => false
        case _: InjectedFault => true
        case other            => loop(other.getCause)
      }
    loop(t)
  }

  test("failure at afterCreateInsert rolls back instance and wakeup; a retry runs the instance") {
    val wf = workflow("wf-atomic-1")
    val rt = runtimeWith(FailingInjector("afterCreateInsert"))

    val thrown = intercept[WorkflowTechnicalException] {
      rt.createAndScheduleForAsyncRun(wf, "key-1", "hello")
    }
    assert(causeChainContains(thrown))

    assertEquals(instanceRows(wf.id, "key-1"), 0L)
    assertEquals(wakeupRows(wf.id, "key-1"), 0L)

    val clean = cleanRuntime
    clean.createAndScheduleForAsyncRun(wf, "key-1", "hello")
    assertEquals(instanceRows(wf.id, "key-1"), 1L)
    assertEquals(wakeupRows(wf.id, "key-1"), 1L)
    assertEquals(runScheduledToCompletion(clean, wf, "key-1"), Some("completed"))
  }

  test("failure at afterWakeupInsert rolls back instance and wakeup; a retry runs the instance") {
    val wf = workflow("wf-atomic-2")
    val rt = runtimeWith(FailingInjector("afterWakeupInsert"))

    val thrown = intercept[WorkflowTechnicalException] {
      rt.createAndScheduleForAsyncRun(wf, "key-2", "hello")
    }
    assert(causeChainContains(thrown))

    assertEquals(instanceRows(wf.id, "key-2"), 0L)
    assertEquals(wakeupRows(wf.id, "key-2"), 0L)

    val clean = cleanRuntime
    clean.createAndScheduleForAsyncRun(wf, "key-2", "hello")
    assertEquals(instanceRows(wf.id, "key-2"), 1L)
    assertEquals(wakeupRows(wf.id, "key-2"), 1L)
    assertEquals(runScheduledToCompletion(clean, wf, "key-2"), Some("completed"))
  }

  test("failure at afterCreateAndScheduleCommit leaves both rows committed; a clean run completes the instance") {
    val wf = workflow("wf-atomic-3")
    val rt = runtimeWith(FailingInjector("afterCreateAndScheduleCommit"))

    intercept[InjectedFault] {
      rt.createAndScheduleForAsyncRun(wf, "key-3", "hello")
    }

    assertEquals(instanceRows(wf.id, "key-3"), 1L)
    assertEquals(wakeupRows(wf.id, "key-3"), 1L)

    assertEquals(runScheduledToCompletion(cleanRuntime, wf, "key-3"), Some("completed"))
  }

  test("normal run creates both rows") {
    val wf = workflow("wf-atomic-4")
    val rt = runtimeWith(FaultInjector.Noop)

    rt.createAndScheduleForAsyncRun(wf, "key-4", "hello")
    assertEquals(instanceRows(wf.id, "key-4"), 1L)
    assertEquals(wakeupRows(wf.id, "key-4"), 1L)
  }
}
