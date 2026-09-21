package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

final case class StepResult(value: String) derives Cacheable

final class MutableClock(var now: Instant) extends Clock {
  override def getZone: ZoneOffset = ZoneOffset.UTC
  override def withZone(zone: java.time.ZoneId): Clock = this
  override def instant(): Instant = now
  def advance(d: FiniteDuration): Unit = now = now.plus(java.time.Duration.ofNanos(d.toNanos))
}

class StepAtLeastOnceSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def stepRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String,
      stepVersion: Long
  ): Option[(String, String, Option[Instant])] =
    run(
      sql"""SELECT state_kind, state_payload, expires_at FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND step_id = $stepId AND step_version = $stepVersion""".query[
          (String, String, Option[Instant])
        ].option
    )

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

  private final class EncoderBombException(msg: String) extends RuntimeException(msg)

  private def refusingThrowableCodec: Cacheable[Throwable] = {
    val base = Cacheable.forThrowable.genericStringMessageSerializer
    new Cacheable[Throwable] {
      override def stableSerializedTypeId: String = "test-refusing"
      override def write(t: Throwable): String = t match {
        case _: EncoderBombException => throw new RuntimeException("refusing to encode")
        case _                       => base.write(t)
      }
      override def read(s: String): Throwable = base.read(s)
    }
  }

  test("body executes once; re-run of a completed workflow returns the cached value without re-executing") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "once") { in =>
      Step.atLeastOnce[String]("step") {
        counter.incrementAndGet()
        s"value-$in"
      }
    }

    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.Result("value-a"))
    assertEquals(counter.get(), 1)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("succeeded"))

    assertEquals(rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k")), WorkflowRunResult.Result("value-a"))
    assertEquals(counter.get(), 1)
  }

  test("the Started row is persisted before the body executes (observable mid-body via SQL)") {
    val rt = newRuntime
    var observed: Option[String] = None
    val wf = Workflow[String, String](id = "started-before") { in =>
      Step.atLeastOnce[String]("step") {
        observed = run(
          sql"""SELECT state_kind FROM workflow_steps
                WHERE workflow_id = 'started-before' AND workflow_instance_key = 'k' AND scope = '' AND step_id = 'step' AND step_version = 1""".query[
              String
            ].option
        )
        "result"
      }
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(observed, Some("started"))
  }

  test("success returns the decoded value; identity is not preserved (commit-before-observation)") {
    val rt = newRuntime
    var captured: StepResult = null
    val wf = Workflow[String, StepResult](id = "identity") { in =>
      Step.atLeastOnce[StepResult]("step") {
        captured = StepResult("x")
        StepResult("x")
      }
    }

    val result = rt.createAndRun(wf, "k", "a")
    result match {
      case WorkflowRunResult.Result(v) =>
        assertEquals(v, StepResult("x"))
        assert(v.ne(captured), "the decoded value must be a distinct instance")
      case other => fail(s"unexpected result: $other")
    }
  }

  test("failure persists a Failed row, throws the decoded failure, and replays it without re-executing") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var caught: Option[String] = None
    val wf = Workflow[String, String](id = "fail") { in =>
      try {
        Step.atLeastOnce[String]("step") {
          counter.incrementAndGet()
          throw new RuntimeException(s"boom-$in")
        }
        "unreachable"
      } catch {
        case e: Throwable =>
          caught = Some(e.getMessage)
          "caught"
      } finally {
        TestControlFlow.suspend()
      }
    }

    rt.createAndRun(wf, "k", "x")
    assertEquals(counter.get(), 1)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("failed"))
    val firstMessage = caught.get
    assert(firstMessage.contains("boom-x"))

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "replay must not re-execute the failed step body")
    assertEquals(caught.get, firstMessage)
  }

  test("a control-flow exception thrown from the body is never cached; run returns WorkflowSuspended") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "control") { in =>
      Step.atLeastOnce[String]("step") {
        TestControlFlow.suspend()
        "never"
      }
    }

    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.WorkflowSuspended)
    val row = stepRow(wf.id, "k", "step", 1)
    assert(row.exists(_._1 == "started"), s"step should remain Started, got $row")
  }

  test("bumping the version creates new retryable work (body re-executes)") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var stepVersion = 1L
    val wf = Workflow[String, String](id = "version") { in =>
      Step.atLeastOnce[String]("step", version = stepVersion) {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("succeeded"))
    assert(stepRow(wf.id, "k", "step", 2).isEmpty)

    stepVersion = 2
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2)
    assertEquals(stepRow(wf.id, "k", "step", 2).map(_._1), Some("succeeded"))
  }

  test("getExecutionState reports NeverStarted") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "state-never") { in =>
      state = Step.getExecutionState[String]("ghost")
      TestControlFlow.suspend()
      "unreachable"
    }
    rt.createAndRun(wf, "k", "a")
    assertEquals(state, StepExecutionState.NeverStarted)
  }

  test("getExecutionState reports Started after a crash leaves a Started row") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "state-started") { in =>
      state = Step.getExecutionState[String]("step", stepVersion = 1)
      Step.atLeastOnce[String]("step") {
        TestControlFlow.suspend()
        "never"
      }
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(state, StepExecutionState.Started)
  }

  test("getExecutionState reports Completed with the decoded value after success") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "state-completed") { in =>
      Step.atLeastOnce[String]("step") {
        "cached-value"
      }
      state = Step.getExecutionState[String]("step", stepVersion = 1)
      TestControlFlow.suspend()
      "unreachable"
    }
    rt.createAndRun(wf, "k", "a")
    assertEquals(state, StepExecutionState.Completed("cached-value"))
  }

  test("getExecutionState reports Failed with the decoded failure") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "state-failed") { in =>
      try {
        Step.atLeastOnce[String]("step") {
          throw new RuntimeException("boom")
        }
      } catch {
        case _: Throwable => ()
      }
      state = Step.getExecutionState[String]("step", stepVersion = 1)
      TestControlFlow.suspend()
      "unreachable"
    }
    rt.createAndRun(wf, "k", "a")
    state match {
      case StepExecutionState.Failed(f) => assert(f.getMessage.contains("boom"))
      case other                       => fail(s"expected Failed, got $other")
    }
  }

  test("a stale step write (fencing token bumped mid-body) throws LeaseLostException") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "fenced") { in =>
      Step.atLeastOnce[String]("step") {
        run(
          sql"""UPDATE workflow_instances SET fencing_token = fencing_token + 1
                WHERE workflow_id = 'fenced' AND workflow_instance_key = 'k' AND scope = ''""".update.run
        )
        "result"
      }
    }

    intercept[LeaseLostException] {
      rt.createAndRun(wf, "k", "a")
    }
    val row = instanceRow(wf.id, "k").get
    assertEquals(row._1, None, "the run must not become terminal after lease loss")
  }

  test("ensureUnchanged: same inputs hit the cache; a changed input raises StepInputConflictException") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var date = "2024-01-01"
    var conflict: Option[String] = None
    val wf = Workflow[String, String](id = "ensure") { in =>
      try {
        Step.atLeastOnce[String]("step", ensureUnchanged = Seq("date" -> date)) {
          counter.incrementAndGet()
          "result"
        }
      } catch {
        case e: StepInputConflictException => conflict = Some(e.getMessage)
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1)

    date = "2024-02-01"
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "ensureUnchanged conflict must not re-execute")
    assert(conflict.isDefined)
  }

  test("invalidateOn: a changed input re-executes and caches the new result") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var market = "EU"
    val wf = Workflow[String, String](id = "invalidate") { in =>
      Step.atLeastOnce[String]("step", invalidateOn = Seq("market" -> market)) {
        counter.incrementAndGet()
        s"result-$market"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1)

    market = "US"
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2, "the new result should be cached")
  }

  test("invalidateAfter: the cached result expires after the TTL and the body re-executes") {
    val clock = new MutableClock(Instant.parse("2024-01-01T00:00:00Z"))
    val rt = newRuntime(clock)
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "ttl") { in =>
      Step.atLeastOnce[String]("step", invalidateAfter = 10.seconds) {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "before expiry the result is cached")

    clock.advance(11.seconds)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2, "after expiry the body re-executes")
  }

  test("no named inputs: the instance id is the sole cache key") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "instance-key") { in =>
      Step.atLeastOnce[String]("step") {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k1", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k1"))
    assertEquals(counter.get(), 1, "same instance replays from cache")

    rt.createAndRun(wf, "k2", "a")
    assertEquals(counter.get(), 2, "a different instance re-executes")
  }

  test("getExecutionState decodes a Failed row with the contextual throwable codec") {
    val rt = newRuntime
    given Cacheable[Throwable] = Cacheable.forThrowable.javaSerializable
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "state-codec") { in =>
      try {
        Step.atLeastOnce[String]("step") {
          throw new RuntimeException("boom")
        }
      } catch {
        case _: Throwable => ()
      }
      state = Step.getExecutionState[String]("step", stepVersion = 1)
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    state match {
      case StepExecutionState.Failed(f) => assert(f.getMessage.contains("boom"))
      case other                       => fail(s"expected Failed, got $other")
    }
  }

  test("a changed ensureUnchanged input against a Started row raises StepInputConflictException") {
    val rt = newRuntime
    var date = "2024-01-01"
    var conflict: Option[String] = None
    val wf = Workflow[String, String](id = "started-conflict") { in =>
      try {
        Step.atLeastOnce[String]("step", ensureUnchanged = Seq("date" -> date)) {
          TestControlFlow.suspend()
          "never"
        }
      } catch {
        case e: StepInputConflictException => conflict = Some(e.getMessage)
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("started"))

    date = "2024-02-01"
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assert(conflict.isDefined, "a changed ensureUnchanged input must conflict even against a Started row")
  }

  test("when the throwable codec cannot encode a failure, a StepSerializationFailed is persisted and replayed") {
    val rt = newRuntime
    given Cacheable[Throwable] = refusingThrowableCodec
    val counter = new AtomicInteger(0)
    var caught: Option[String] = None
    val wf = Workflow[String, String](id = "enc-fail") { in =>
      try {
        Step.atLeastOnce[String]("step") {
          counter.incrementAndGet()
          throw new EncoderBombException("boom")
        }
      } catch {
        case e: Throwable => caught = Some(e.getMessage)
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    assert(caught.get.contains("could not be encoded"), s"unexpected: $caught")
    assertEquals(stepRow(wf.id, "k", "step", 1).map(_._1), Some("failed"))

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "replay must not re-execute the failed step body")
    assert(caught.get.contains("could not be encoded"), s"unexpected: $caught")
  }

  test("multi-input invalidateOn: only the changed input invalidates; colliding values under distinct names are independent") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var a = "x"
    var b = "x"
    val wf = Workflow[String, String](id = "multi") { in =>
      Step.atLeastOnce[String]("step", invalidateOn = Seq("a" -> a, "b" -> b)) {
        counter.incrementAndGet()
        "result"
      }
      TestControlFlow.suspend()
      "unreachable"
    }

    rt.createAndRun(wf, "k", "a")
    assertEquals(counter.get(), 1)
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "identical inputs hit the cache even when two names hold colliding values")

    a = "y"
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2, "changing only one of two inputs must re-execute")
    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 2, "the new result is cached")
  }
}
