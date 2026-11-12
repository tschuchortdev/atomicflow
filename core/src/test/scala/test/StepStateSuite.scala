package test

import atomicflow.*
import munit.FunSuite

import java.time.Instant

class StepStateSuite extends FunSuite {

  private val deadline = Instant.parse("2026-01-01T00:00:00Z")

  private val allStates: List[StepState] = List(
    StepState.SimpleStep.Started,
    StepState.SimpleStep.RetryPending(SerializedValue.of("bookkeeping"), deadline),
    StepState.SimpleStep.Succeeded(SerializedValue.of("result")),
    StepState.SimpleStep.Failed(SerializedValue.of("failure")),
    StepState.Await(SerializedValue.of("await-result")),
    StepState.FirstCompleted("branch", SerializedValue.of("winner"), hasUnfinishedBranches = true),
    StepState.FirstCompleted("branch", SerializedValue.of("winner"), hasUnfinishedBranches = false),
    StepState.Region(RegionState(3, SerializedValue.of("region-state")))
  )

  test("persistedColumns and fromPersisted round-trip every state kind") {
    allStates.foreach { state =>
      val (stateKind, payload, retryDeadline) = state.persistedColumns
      assertEquals(
        StepState.fromPersisted(state.rowKindText, stateKind, payload, retryDeadline),
        state
      )
    }
  }

  test("FirstCompleted persists its branch key, value, and unfinished flag") {
    List(true, false).foreach { hasUnfinishedBranches =>
      val state = StepState.FirstCompleted("branch", SerializedValue.of("winner"), hasUnfinishedBranches)
      val (stateKind, payload, retryDeadline) = state.persistedColumns
      val decoded = StepState.fromPersisted(state.rowKindText, stateKind, payload, retryDeadline)
      assertEquals(decoded, state)
      decoded match {
        case StepState.FirstCompleted(branch, result, unfinished) =>
          assertEquals(branch, "branch")
          assertEquals(result, SerializedValue.of("winner"))
          assertEquals(unfinished, hasUnfinishedBranches)
        case other => fail(s"expected FirstCompleted, got $other")
      }
    }
  }

  test("rowKindText records the step kind that wrote the row") {
    assertEquals(StepState.SimpleStep.Started.rowKindText, StepKind.Step)
    assertEquals(StepState.Await(SerializedValue.of("x")).rowKindText, StepKind.Await)
    assertEquals(StepState.FirstCompleted("branch", SerializedValue.of("x"), hasUnfinishedBranches = false).rowKindText, StepKind.FirstCompleted)
    assertEquals(
      StepState.Region(RegionState(0, SerializedValue.of("x"))).rowKindText,
      StepKind.RestartableRegion
    )
  }

  test("pendingRetryDeadline is set only for a retry-pending state") {
    assertEquals(
      StepState.SimpleStep.RetryPending(SerializedValue.of("bk"), deadline).pendingRetryDeadline,
      Some(deadline)
    )
    allStates.filterNot(_.isInstanceOf[StepState.SimpleStep.RetryPending]).foreach { state =>
      assertEquals(state.pendingRetryDeadline, None)
    }
  }

  test("fromPersisted rejects RetryPending without a retry deadline") {
    val e = intercept[IllegalArgumentException] {
      StepState.fromPersisted(StepKind.Step, "RetryPending", "bk", None)
    }
    assert(e.getMessage.contains("missing its retry_deadline"))
  }

  test("fromPersisted rejects state kinds that are illegal for each row kind") {
    val illegal: List[(String, String)] = List(
      StepKind.Step -> "Bogus",
      StepKind.Await -> "Started",
      StepKind.Await -> "Failed",
      StepKind.FirstCompleted -> "Failed",
      StepKind.RestartableRegion -> "Succeeded"
    )
    illegal.foreach { (rowKind, stateKind) =>
      val e = intercept[IllegalArgumentException] {
        StepState.fromPersisted(rowKind, stateKind, "", None)
      }
      assert(
        e.getMessage.contains(s"state_kind '$stateKind' is not valid for a '$rowKind' row"),
        e.getMessage
      )
    }
  }

  test("fromPersisted rejects an unknown row kind") {
    val e = intercept[IllegalArgumentException] {
      StepState.fromPersisted("Nope", "Succeeded", "", None)
    }
    assert(e.getMessage.contains("Unknown step_kind 'Nope'"))
  }

  test("RegionState encode/decode preserves the restart count and serialized state") {
    val state = RegionState(7, SerializedValue.of("""{"value":42}"""))
    assertEquals(RegionState.decode(RegionState.encode(state)), state)
  }
}
