package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import munit.FunSuite

object TypesSuiteFixtures:
  case class PaymentDeclinedFixture(reason: String) extends RuntimeException(reason) derives Cacheable

class TypesSuite extends FunSuite:
  import TypesSuiteFixtures.*

  test("WorkflowInstanceId defaults to the top-level empty scope") {
    val id = WorkflowInstanceId("orders", "order-42")
    assertEquals(id.workflowId, "orders")
    assertEquals(id.workflowInstanceKey, "order-42")
    assertEquals(id.scope, "")
    assertEquals(WorkflowInstanceId("orders", "order-42"), WorkflowInstanceId("orders", "order-42"))
    assertNotEquals(WorkflowInstanceId("orders", "order-42"), WorkflowInstanceId("orders", "order-42", "s"))
  }

  test("StepId defaults to the empty scope") {
    assertEquals(StepId("charge").scope, "")
    assertEquals(StepId("charge"), StepId("charge", ""))
  }

  test("WorkflowCompletionResult codec round-trips all four cases") {
    given throwable: Cacheable[Throwable] = Cacheable.forThrowable.javaSerializable

    assertEquals(
      summon[Cacheable[WorkflowCompletionResult[Int]]].read(
        summon[Cacheable[WorkflowCompletionResult[Int]]].write(WorkflowCompletionResult.Completed(42))
      ),
      WorkflowCompletionResult.Completed(42)
    )

    val failure = PaymentDeclinedFixture("no funds")
    assertEquals(
      summon[Cacheable[WorkflowCompletionResult[Int]]].read(
        summon[Cacheable[WorkflowCompletionResult[Int]]].write(WorkflowCompletionResult.Failed(failure))
      ),
      WorkflowCompletionResult.Failed(failure)
    )

    assertEquals(
      summon[Cacheable[WorkflowCompletionResult[Int]]].read(
        summon[Cacheable[WorkflowCompletionResult[Int]]].write(WorkflowCompletionResult.Cancelled)
      ),
      WorkflowCompletionResult.Cancelled
    )
    assertEquals(
      summon[Cacheable[WorkflowCompletionResult[Int]]].read(
        summon[Cacheable[WorkflowCompletionResult[Int]]].write(WorkflowCompletionResult.Terminated)
      ),
      WorkflowCompletionResult.Terminated
    )
  }

  test("WorkflowCompletionResult codec records the member codec ids") {
    given throwable: Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer
    val c = summon[Cacheable[WorkflowCompletionResult[Int]]]
    assertEquals(
      c.stableSerializedTypeId,
      s"workflow-completion-result(${summon[Cacheable[Int]].stableSerializedTypeId},throwable-generic)"
    )
    // a Failed payload written with the generic codec decodes to StepFailed
    c.read(c.write(WorkflowCompletionResult.Failed(new IllegalStateException("x")))) match
      case WorkflowCompletionResult.Failed(_: StepFailed) => // expected
      case other => fail(s"expected Failed(StepFailed), got $other")
  }

