package atomicflow

import atomicflow.internal.Framing

/** The terminal outcome of a workflow instance, as stored in the
  * `WorkflowCompleted` event and awaited by parents (see `running-workflows.md`,
  * "Terminal outcome storage").
  *
  * `TERMINATED` is a real terminal state, not a flavor of cancellation: only
  * `cancel` gives the body a chance to react; `terminate` stops the instance
  * without running user code.
  */
enum WorkflowCompletionResult[+R] {
  case Completed[R](result: R) extends WorkflowCompletionResult[R]
  case Failed(failure: Throwable) extends WorkflowCompletionResult[Nothing]
  case Cancelled extends WorkflowCompletionResult[Nothing]
  case Terminated extends WorkflowCompletionResult[Nothing]
}

object WorkflowCompletionResult {

  /** Composite codec built from the result and throwable codecs; records the
    * selected member codec ids per the `Cacheable` composition scheme.
    */
  given [R: Cacheable](using throwableCacheable: Cacheable[Throwable]): Cacheable[WorkflowCompletionResult[R]] =
    new Cacheable[WorkflowCompletionResult[R]] {
      private val resultCacheable = summon[Cacheable[R]]

      override def stableSerializedTypeId: String =
        s"workflow-completion-result(${resultCacheable.stableSerializedTypeId},${throwableCacheable.stableSerializedTypeId})"

      override def write(value: WorkflowCompletionResult[R]): String = value match {
        case WorkflowCompletionResult.Completed(result) =>
          writeMember("completed", resultCacheable, result)
        case WorkflowCompletionResult.Failed(failure) =>
          writeMember("failed", throwableCacheable, failure)
        case WorkflowCompletionResult.Cancelled => Framing.write("cancelled")
        case WorkflowCompletionResult.Terminated => Framing.write("terminated")
      }

      override def read(serialized: String): WorkflowCompletionResult[R] = {
        val (tag, afterTag) = Framing.read(serialized, 0)
        tag match {
          case "completed" =>
            val (result, _) = readMember(serialized, afterTag, resultCacheable)
            WorkflowCompletionResult.Completed(result)
          case "failed" =>
            val (failure, _) = readMember(serialized, afterTag, throwableCacheable)
            WorkflowCompletionResult.Failed(failure)
          case "cancelled" => WorkflowCompletionResult.Cancelled
          case "terminated" => WorkflowCompletionResult.Terminated
          case other => throw new IllegalArgumentException(s"Unknown WorkflowCompletionResult tag: $other")
        }
      }

      private def writeMember[A](tag: String, member: Cacheable[A], value: A): String =
        Framing.write(tag) + Framing.write(member.stableSerializedTypeId) + Framing.write(member.write(value))

      private def readMember[A](serialized: String, offset: Int, member: Cacheable[A]): (A, Int) = {
        val (memberId, afterId) = Framing.read(serialized, offset)
        if (memberId != member.stableSerializedTypeId)
          throw new IllegalArgumentException(
            s"WorkflowCompletionResult member codec mismatch: stored=$memberId, expected=${member.stableSerializedTypeId}"
          )
        val (payload, afterPayload) = Framing.read(serialized, afterId)
        (member.read(payload), afterPayload)
      }
    }
}
