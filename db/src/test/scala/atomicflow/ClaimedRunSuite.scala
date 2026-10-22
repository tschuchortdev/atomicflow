package atomicflow

import atomicflow.Cacheable.Simple.given
import test.PostgresWorkflowRuntimeSuite

/** Race handling in the job runner's claimed-run path: the instance can be
  * deleted or reach a terminal state between the claim and the run.
  */
class ClaimedRunSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  test("a claimed run of a deleted instance raises WorkflowNotFoundException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "claimed-gone")(in => in)

    intercept[WorkflowNotFoundException] {
      rt.runClaimedInstance(wf, WorkflowInstanceId(wf.id, "missing"), "worker", 0L)
    }
  }

  test("a claimed run of an already-terminal instance decodes its outcome") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "claimed-terminal")(in => s"out-$in")
    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("out-a"))

    assertEquals(
      rt.runClaimedInstance(wf, WorkflowInstanceId(wf.id, "k"), "worker", 0L),
      WorkflowRunResult.Result("out-a")
    )
  }
}
