package atomicflow

import munit.FunSuite

class ControlFlowSuite extends FunSuite:

  test("WorkflowNonFatal excludes library control-flow exceptions") {
    assertEquals(WorkflowNonFatal.unapply(WorkflowSuspendedException()), None)
    assertEquals(WorkflowNonFatal.unapply(ContinueAsNewException("input")), None)
  }

  test("WorkflowNonFatal includes WorkflowCancelledException so cleanup code can catch it") {
    assertEquals(WorkflowNonFatal.unapply(WorkflowCancelledException()).map(_.getClass),
      Some(classOf[WorkflowCancelledException]))
  }

  test("WorkflowNonFatal includes ordinary exceptions and interrupts") {
    assertEquals(WorkflowNonFatal.unapply(new RuntimeException).isDefined, true)
    assertEquals(WorkflowNonFatal.unapply(new InterruptedException).isDefined, true)
  }

  test("WorkflowNonFatal excludes fatal JVM errors") {
    assertEquals(WorkflowNonFatal.unapply(new OutOfMemoryError), None)
    assertEquals(WorkflowNonFatal.unapply(new StackOverflowError), None)
  }

  test("scala.util.control.NonFatal also excludes the library control-flow exceptions") {
    // broad user cleanup with plain NonFatal already rethrows our control flow,
    // because they extend ControlThrowable (munit's intercept cannot catch
    // ControlThrowable, so we assert manually)
    var matched = false
    try
      try throw WorkflowSuspendedException()
      catch case WorkflowNonFatal(e) => matched = true
    catch case _: WorkflowSuspendedException => ()
    assert(!matched, "WorkflowNonFatal must not match WorkflowSuspendedException")
  }
