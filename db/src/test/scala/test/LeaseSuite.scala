package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import scala.concurrent.duration.*

class LeaseSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def leaseExpiry(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""SELECT lease_expires_at FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[Option[Instant]].option
    ).flatten

  private def instanceRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Option[String], Long)] =
    run(
      sql"""SELECT terminal_state, terminal_outcome, fencing_token FROM workflow_instances
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = ''""".query[
          (Option[String], Option[String], Long)
        ].option
    )

  test("Workflow.heartbeat inside a step body extends lease_expires_at") {
    val clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"))
    val rt = newRuntime(clock)
    var before: Option[Instant] = None
    var after: Option[Instant] = None
    val wf = Workflow[String, String](id = "hb") { in =>
      Step.atLeastOnce[String]("step") {
        before = leaseExpiry("hb", "k")
        clock.advance(1.minute)
        Workflow.heartbeat()
        after = leaseExpiry("hb", "k")
        "result"
      }
    }

    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("result"))
    assert(before.isDefined, "the lease should be held when the step body starts")
    assert(after.isDefined, "the lease should remain held after heartbeat")
    assert(after.get.isAfter(before.get), s"heartbeat must extend the lease: before=$before after=$after")
    assertEquals(after, Some(clock.instant().plus(java.time.Duration.ofMinutes(5))))
  }

  test("Workflow.heartbeat after a SQL fencing-token bump throws LeaseLostException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "hb-fenced") { in =>
      Step.atLeastOnce[String]("step") {
        run(
          sql"""UPDATE workflow_instances SET fencing_token = fencing_token + 1
                WHERE workflow_id = 'hb-fenced' AND workflow_instance_key = 'k' AND scope = ''""".update.run
        )
        Workflow.heartbeat()
        "result"
      }
    }

    intercept[LeaseLostException] {
      rt.createAndRun(wf, "k", "a")
    }
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, None, "the run must not become terminal after lease loss")
  }

  test("an expired lease is acquirable by an external run, bumping the fencing token") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "expired")(in => s"out-$in")
    rt.createWorkflowInstance(wf, "k", "a")
    run(
      sql"""UPDATE workflow_instances SET lease_owner = 'someone-else', lease_expires_at = now() - interval '1 hour'
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND scope = ''""".update.run
    )

    val result = rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(result, WorkflowRunResult.Result("out-a"))
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, Some("completed"))
    assert(row._3 > 0, "the fencing token must be bumped on acquisition")
  }

  test("WorkflowNonFatal matches LeaseLostException and LeaseUnavailableException") {
    val wf = Workflow[String, String](id = "nonfatal")(in => in)
    val id = WorkflowInstanceId(wf.id, "k")
    assert(WorkflowNonFatal.unapply(LeaseLostException.apply(id)).isDefined)
    assert(WorkflowNonFatal.unapply(LeaseUnavailableException.apply(id)).isDefined)
  }

  test("Workflow.versionAtCreation exposes the version recorded at creation") {
    val rt = newRuntime
    var v: Long = -1L
    val wf = Workflow[String, String](id = "version", version = 7L) { in =>
      v = Workflow.versionAtCreation
      in
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(v, 7L)
  }
}
