package atomicflow.internal

import atomicflow.{WorkflowId, WorkflowInstanceKey}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Composition and normalization of derived workflow-instance scopes (see
  * `spec/sub-workflows-iteration.md`, "Key derivation").
  *
  * The scope is a flat string whose structural delimiters are `/` (segment
  * boundary) and `@` (generation / restart-count marker, always numeric). Every
  * user-supplied segment is backslash-escaped before joining, making the
  * derivation injective.
  */
private[atomicflow] object ScopePath {

  /** The maximum encoded length of a scope portion before it is replaced by a
    * marked stable hash.
    */
  val MaxEncodedLength: Int = 512

  /** Escapes `\`, `/` and `@` so a segment cannot be confused with structural
    * delimiters. The escaped `\` keeps the encoding unambiguous.
    */
  def escapeScopeSegment(s: String): String =
    s.replace("\\", "\\\\").replace("/", "\\/").replace("@", "\\@")

  /** Joins already-escaped segments with the `/` delimiter, outer-to-inner. */
  def joinEscaped(escapedSegments: String*): String = escapedSegments.mkString("/")

  /** Derives a child workflow instance's scope from its parent's identity and
    * the current run context: the parent's own instance scope, the parent
    * workflow id and instance key (backslash-escaped), the parent's current run
    * generation as an `@generation` marker, and the parent's enclosing
    * `Workflow.scoped` path. The result is injective because every user-supplied
    * segment is escaped (see `spec/sub-workflows-iteration.md`, "Key
    * derivation").
    */
  def deriveChildScope(
      parentInstanceScope: String,
      parentWorkflowId: WorkflowId,
      parentKey: WorkflowInstanceKey,
      parentGeneration: Long,
      enclosingScopePath: String
  ): String = {
    val segments = Vector.newBuilder[String]
    if (parentInstanceScope.nonEmpty) segments += parentInstanceScope
    segments += escapeScopeSegment(parentWorkflowId)
    segments += escapeScopeSegment(parentKey) + "@" + parentGeneration
    if (enclosingScopePath.nonEmpty) segments += enclosingScopePath
    joinEscaped(segments.result()*)
  }

  /** Replaces an overlong scope portion with a marked stable hash, so all children
    * of one parent instance and generation begin with the same normalized prefix.
    * Portions at or below [[MaxEncodedLength]] are returned unchanged.
    */
  def shortenIfTooLong(scopePortion: String): String =
    if (scopePortion.length <= MaxEncodedLength) scopePortion
    else {
      val digest = MessageDigest.getInstance("SHA-256").digest(scopePortion.getBytes(StandardCharsets.UTF_8))
      s"shortened[sha256:${digest.map(b => f"${b & 0xff}%02x").mkString}]"
    }
}
