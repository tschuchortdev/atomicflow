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

  test("failure at afterCreateInsert rolls back instance and wakeup; surfaces WorkflowTechnicalException") {
    val wf = workflow("wf-atomic-1")
    val rt = runtimeWith(FailingInjector("afterCreateInsert"))

    val thrown = intercept[WorkflowTechnicalException] {
      rt.createAndScheduleForAsyncRun(wf, "key-1", "hello")
    }
    assert(causeChainContains(thrown))

    assertEquals(instanceRows(wf.id, "key-1"), 0L)
    assertEquals(wakeupRows(wf.id, "key-1"), 0L)
  }

  test("failure at afterWakeupInsert rolls back instance and wakeup") {
    val wf = workflow("wf-atomic-2")
    val rt = runtimeWith(FailingInjector("afterWakeupInsert"))

    val thrown = intercept[WorkflowTechnicalException] {
      rt.createAndScheduleForAsyncRun(wf, "key-2", "hello")
    }
    assert(causeChainContains(thrown))

    assertEquals(instanceRows(wf.id, "key-2"), 0L)
    assertEquals(wakeupRows(wf.id, "key-2"), 0L)
  }

  test("failure at afterCreateAndScheduleCommit leaves both rows committed") {
    val wf = workflow("wf-atomic-3")
    val rt = runtimeWith(FailingInjector("afterCreateAndScheduleCommit"))

    intercept[InjectedFault] {
      rt.createAndScheduleForAsyncRun(wf, "key-3", "hello")
    }

    assertEquals(instanceRows(wf.id, "key-3"), 1L)
    assertEquals(wakeupRows(wf.id, "key-3"), 1L)
  }

  test("normal run creates both rows") {
    val wf = workflow("wf-atomic-4")
    val rt = runtimeWith(FaultInjector.Noop)

    rt.createAndScheduleForAsyncRun(wf, "key-4", "hello")
    assertEquals(instanceRows(wf.id, "key-4"), 1L)
    assertEquals(wakeupRows(wf.id, "key-4"), 1L)
  }
}
