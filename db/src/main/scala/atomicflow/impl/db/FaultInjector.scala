package atomicflow.impl.db

/** Test seam for injecting faults at the durable boundaries of the atomic
  * create-and-schedule transaction. Each hook runs inside the transaction
  * (except `afterCreateAndScheduleCommit`, which runs after commit), so a throw
  * from an in-transaction hook rolls the whole transaction back. Because
  * `afterCreateInsert` lives in the shared create helper, it also fires for a
  * plain `createWorkflowInstance` that actually inserts, not only for
  * create-and-schedule.
  */
private[atomicflow] trait FaultInjector {
  def afterCreateInsert(): Unit = ()
  def afterWakeupInsert(): Unit = ()
  def afterCreateAndScheduleCommit(): Unit = ()
}

private[atomicflow] object FaultInjector { object Noop extends FaultInjector }
