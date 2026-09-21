package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

final case class QuoteV1(symbol: String, price: Double) derives Cacheable
final case class QuoteLegacy(symbol: String) derives Cacheable
final case class QuoteChain(symbol: String, ver: Int) derives Cacheable

private final class FatalMarker(msg: String) extends LinkageError(msg)

class EvolutionSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def stepPayload(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      stepId: String,
      stepVersion: Long
  ): Option[String] =
    run(
      sql"""SELECT state_payload FROM workflow_steps
            WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND scope = '' AND step_id = $stepId AND step_version = $stepVersion""".query[
          String
        ].option
    )

  test("versionAtCreation branches durable behavior; Info.workflowVersionAtCreation matches") {
    val rt = newRuntime

    def mkDef(version: Long): Workflow[String, String] =
      Workflow[String, String](id = "evo-branch", version = version) { in =>
        if (Workflow.versionAtCreation >= 2)
          Step.atLeastOnce[String]("which") { "v2-" + in }
        else
          Step.atLeastOnce[String]("which") { "v1-" + in }
        TestControlFlow.suspend()
        "unreachable"
      }

    val wf1 = mkDef(1)
    val handle1 = rt.createWorkflowInstance(wf1, "k1", "a")
    assertEquals(handle1.getInfo()(using rt).workflowVersionAtCreation, 1L)
    assertEquals(rt.runWorkflowInstance(wf1, handle1.id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(stepPayload(wf1.id, "k1", "which", 1), Some("v1-a"))

    val wf2 = mkDef(2)
    val handle2 = rt.createWorkflowInstance(wf2, "k2", "b")
    assertEquals(handle2.getInfo()(using rt).workflowVersionAtCreation, 2L)
    assertEquals(rt.runWorkflowInstance(wf2, handle2.id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(stepPayload(wf2.id, "k2", "which", 1), Some("v2-b"))

    // A v1-created instance, suspended mid-run, re-run under a v2 definition must
    // still read the creation-time version from the instance row (1), not the
    // version of the definition now driving the run (2).
    val wf2re = Workflow[String, String](id = "evo-branch", version = 2) { in =>
      Step.atLeastOnce[String]("which2") { "at" + Workflow.versionAtCreation + "-" + in }
      TestControlFlow.suspend()
      "unreachable"
    }
    assertEquals(rt.runWorkflowInstance(wf2re, handle1.id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(stepPayload(wf1.id, "k1", "which2", 1), Some("at1-a"))
    assertEquals(handle1.getInfo()(using rt).workflowVersionAtCreation, 1L)
  }

  test("unconditional code change reaches unfinished instances; completed step cached results are reused") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)

    val wfA = Workflow[String, String](id = "evo-hotfix") { in =>
      Step.atLeastOnce[String]("quote") { counter.incrementAndGet(); "quote-A" }
      Step.atLeastOnce[String]("gate") { TestControlFlow.suspend(); "never" }
      "unreachable"
    }
    assertEquals(rt.createAndRun(wfA, "k", "a"), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)

    val wfA2 = Workflow[String, String](id = "evo-hotfix") { in =>
      val q = Step.atLeastOnce[String]("quote") { counter.incrementAndGet(); "quote-A2" }
      Step.atLeastOnce[String]("gate") { "gate-open" }
      q + "/" + "gate-open"
    }
    assertEquals(
      rt.runWorkflowInstance(wfA2, WorkflowInstanceId(wfA.id, "k")),
      WorkflowRunResult.Result("quote-A/gate-open")
    )
    assertEquals(counter.get(), 1, "the completed quote step's cached result must be reused, not re-executed")
  }

  test("bumping a step version creates fresh work; exact-version lookup does not mix versions") {
    val rt = newRuntime
    val counter = new AtomicInteger(0)
    var stepVersion = 1L
    var stateV1: StepExecutionState[String] = null
    var stateV2: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "evo-version") { in =>
      stateV1 = Step.getExecutionState[String]("quote", stepVersion = 1)
      stateV2 = Step.getExecutionState[String]("quote", stepVersion = 2)
      Step.atLeastOnce[String]("quote", version = stepVersion) { counter.incrementAndGet(); s"v-$stepVersion" }
      TestControlFlow.suspend()
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 1)
    assertEquals(stepPayload(wf.id, "k", "quote", 1), Some("v-1"))
    assert(stepPayload(wf.id, "k", "quote", 2).isEmpty)
    assertEquals(stateV1, StepExecutionState.NeverStarted)
    assertEquals(stateV2, StepExecutionState.NeverStarted)

    stepVersion = 2
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "bumping the version must re-execute the body")
    assertEquals(stepPayload(wf.id, "k", "quote", 1), Some("v-1"), "the v1 row must be untouched")
    assertEquals(stepPayload(wf.id, "k", "quote", 2), Some("v-2"))
    assertEquals(stateV1, StepExecutionState.Completed("v-1"))
    assertEquals(stateV2, StepExecutionState.NeverStarted)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(counter.get(), 2, "replay must reuse both cached rows")
    assertEquals(stateV1, StepExecutionState.Completed("v-1"))
    assertEquals(stateV2, StepExecutionState.Completed("v-2"))
  }

  test("atMostOnce has no version: exercised as an unversioned step and read back by getExecutionState") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "evo-mo") { in =>
      state = Step.getExecutionState[String]("charge")
      Step.atMostOnce[String]("charge") { "charged-" + in }
      TestControlFlow.suspend()
      "unreachable"
    }
    assertEquals(rt.createAndRun(wf, "k", "a"), WorkflowRunResult.WorkflowSuspended)
    assertEquals(state, StepExecutionState.NeverStarted)
    assertEquals(stepPayload(wf.id, "k", "charge", 0), Some("charged-a"))

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(state, StepExecutionState.Completed("charged-a"))
  }

  test("getExecutionState matrix: NeverStarted, at-least-once Started mid-retry, Failed on exhaustion") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    var observed: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "evo-retry-matrix") { in =>
      observed = Step.getExecutionState[String]("quote", stepVersion = 1)
      Step.atLeastOnce[String]("quote", retry = Step.RetryPolicy.fixedDelay(2, 1.hour)) {
        throw new RuntimeException("boom")
      }
      TestControlFlow.suspend()
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(observed, StepExecutionState.NeverStarted, "the step has not started yet")

    clock.advanceBy(30.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(observed, StepExecutionState.Started, "a step mid-durable-retry is Started")
  }

  test("getExecutionState reports Failed once the retry budget is exhausted (durable failure)") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, durableRetryThreshold = 10.millis)
    given Clock = clock
    var observed: StepExecutionState[String] = null
    val wf = Workflow[String, String](id = "evo-retry-failed") { in =>
      try {
        Step.atLeastOnce[String]("quote", retry = Step.RetryPolicy.fixedDelay(1, 1.hour)) {
          throw new RuntimeException("fatal-boom")
        }
      } catch { case _: Throwable => () }
      observed = Step.getExecutionState[String]("quote", stepVersion = 1)
      TestControlFlow.suspend()
      "unreachable"
    }
    val id = rt.createWorkflowInstance(wf, "k", "a").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    clock.advanceBy(2.hours)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    observed match {
      case StepExecutionState.Failed(f) => assert(f.getMessage.contains("fatal-boom"))
      case other                       => fail(s"expected Failed, got $other")
    }
  }

  test("getExecutionState reports Started for an at-most-once body that crashed after the Started row") {
    val rt = newRuntime
    var state: StepExecutionState[String] = null
    var replayed: Option[String] = null
    val counter = new AtomicInteger(0)
    val wf = Workflow[String, String](id = "evo-mo-crash") { in =>
      state = Step.getExecutionState[String]("charge")
      replayed = Step.atMostOnce[String]("charge") {
        counter.incrementAndGet()
        throw new FatalMarker("fatal")
      }
      "unreachable"
    }
    try rt.createAndRun(wf, "k", "a")
    catch { case _: FatalMarker => () }
    assertEquals(counter.get(), 1, "the body must have run exactly once before the crash")

    rt.runWorkflowInstance(wf, WorkflowInstanceId(wf.id, "k"))
    assertEquals(counter.get(), 1, "the crashed at-most-once step must not re-execute on replay")
    assertEquals(replayed, None, "an unresolved Started row yields the at-most-once no-retry None outcome")
    assertEquals(state, StepExecutionState.Started)
  }

  test("getExecutionState decodes a Completed value via the required Cacheable") {
    val rt = newRuntime
    var state: StepExecutionState[QuoteV1] = null
    val wf = Workflow[String, QuoteV1](id = "evo-completed") { in =>
      Step.atLeastOnce[QuoteV1]("quote") { QuoteV1(in, 1.5) }
      state = Step.getExecutionState[QuoteV1]("quote", stepVersion = 1)
      TestControlFlow.suspend()
      QuoteV1("unreachable", -1.0)
    }
    rt.createAndRun(wf, "k", "AAPL")
    assertEquals(state, StepExecutionState.Completed(QuoteV1("AAPL", 1.5)))
  }

  test("cacheable evolution end-to-end: fallback decodes a v1-format result; new writes use the current serializer") {
    def legacyCodec: Cacheable[QuoteV1] = {
      val legacy = summon[Cacheable[QuoteLegacy]]
      legacy.imap(l => QuoteV1(l.symbol, 0.0))(q => QuoteLegacy(q.symbol))
    }
    def currentCodec: Cacheable[QuoteV1] =
      summon[Cacheable[QuoteV1]].withFallback(legacyCodec)

    val rt = newRuntime
    var state: StepExecutionState[QuoteV1] = null

    {
      given Cacheable[QuoteV1] = legacyCodec
      val wfWrite = Workflow[String, QuoteV1](id = "evo-cache") { in =>
        Step.atLeastOnce[QuoteV1]("quote") { QuoteV1(in, 0.0) }
        TestControlFlow.suspend()
        QuoteV1("unreachable", -1.0)
      }
      rt.createAndRun(wfWrite, "k", "AAPL")
    }

    assertEquals(stepPayload("evo-cache", "k", "quote", 1), Some("""{"symbol":"AAPL"}"""))

    {
      given Cacheable[QuoteV1] = currentCodec
      val wfRead = Workflow[String, QuoteV1](id = "evo-cache") { in =>
        state = Step.getExecutionState[QuoteV1]("quote", stepVersion = 1)
        Step.atLeastOnce[QuoteV1]("quote") { QuoteV1("must-not-run", 1.0) }
        Step.atLeastOnce[QuoteV1]("quote2") { QuoteV1("fresh", 42.0) }
        TestControlFlow.suspend()
        QuoteV1("unreachable", -1.0)
      }
      rt.runWorkflowInstance(wfRead, WorkflowInstanceId(wfRead.id, "k"))
    }

    assertEquals(state, StepExecutionState.Completed(QuoteV1("AAPL", 0.0)))
    assertEquals(
      stepPayload("evo-cache", "k", "quote", 1),
      Some("""{"symbol":"AAPL"}"""),
      "the old row must remain in v1 format"
    )
    val fresh = stepPayload("evo-cache", "k", "quote2", 1)
    assert(
      fresh.exists(p => p.contains(""""price"""") && p.contains(""""fresh"""")),
      s"new writes must use the current serializer: $fresh"
    )
  }

  test("cacheable evolution: a v1→v2→current fallback chain decodes each format to its own version") {
    val v1Codec: Cacheable[QuoteChain] =
      summon[Cacheable[QuoteLegacy]].imap(l => QuoteChain(l.symbol, 1))(q => QuoteLegacy(q.symbol))
    val v2Codec: Cacheable[QuoteChain] =
      summon[Cacheable[QuoteV1]].imap(v => QuoteChain(v.symbol, 2))(q => QuoteV1(q.symbol, 0.0))
    val currentCodec: Cacheable[QuoteChain] =
      summon[Cacheable[QuoteChain]].withFallback(v2Codec).withFallback(v1Codec)

    val rt = newRuntime

    {
      given Cacheable[QuoteChain] = v1Codec
      val wfWrite = Workflow[String, QuoteChain](id = "evo-chain") { in =>
        Step.atLeastOnce[QuoteChain]("q") { QuoteChain(in, 1) }
        TestControlFlow.suspend()
        QuoteChain("unreachable", -1)
      }
      rt.createAndRun(wfWrite, "k1", "AAPL")
    }
    assertEquals(stepPayload("evo-chain", "k1", "q", 1), Some("""{"symbol":"AAPL"}"""))

    var state1: StepExecutionState[QuoteChain] = null
    {
      given Cacheable[QuoteChain] = currentCodec
      val wfRead = Workflow[String, QuoteChain](id = "evo-chain") { in =>
        state1 = Step.getExecutionState[QuoteChain]("q", stepVersion = 1)
        TestControlFlow.suspend()
        QuoteChain("unreachable", -1)
      }
      rt.runWorkflowInstance(wfRead, WorkflowInstanceId("evo-chain", "k1"))
    }
    assertEquals(
      state1,
      StepExecutionState.Completed(QuoteChain("AAPL", 1)),
      "a v1 row must fall through current and v2 and decode via the v1 fallback"
    )

    {
      given Cacheable[QuoteChain] = v2Codec
      val wfWrite = Workflow[String, QuoteChain](id = "evo-chain") { in =>
        Step.atLeastOnce[QuoteChain]("q") { QuoteChain(in, 2) }
        TestControlFlow.suspend()
        QuoteChain("unreachable", -1)
      }
      rt.createAndRun(wfWrite, "k2", "AAPL")
    }
    val v2Payload = stepPayload("evo-chain", "k2", "q", 1)
    assert(v2Payload.exists(_.contains(""""price"""")), s"v2 format must carry the price field: $v2Payload")

    var state2: StepExecutionState[QuoteChain] = null
    {
      given Cacheable[QuoteChain] = currentCodec
      val wfRead = Workflow[String, QuoteChain](id = "evo-chain") { in =>
        state2 = Step.getExecutionState[QuoteChain]("q", stepVersion = 1)
        TestControlFlow.suspend()
        QuoteChain("unreachable", -1)
      }
      rt.runWorkflowInstance(wfRead, WorkflowInstanceId("evo-chain", "k2"))
    }
    assertEquals(
      state2,
      StepExecutionState.Completed(QuoteChain("AAPL", 2)),
      "a v2 row must decode via the v2 fallback (before v1) in the chain"
    )
  }
}
