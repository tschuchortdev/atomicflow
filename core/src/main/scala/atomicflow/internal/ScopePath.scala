package atomicflow.internal

import atomicflow.{WorkflowId, WorkflowInstanceKey}

/** Builds child workflow scopes from parent identities and enclosing step scopes.
  *
  * The scope is a segment array: each element is a raw (unescaped) segment, and
  * structure comes from the array position rather than from delimiters, so no
  * user-supplied content needs escaping. Parent markers are `key@generation`
  * (a trailing numeric run) and region markers are `id@restartCount`, formats
  * that stay injective without escaping because a segment boundary is
  * structural, not textual.
  */
private[atomicflow] object ScopePath {

  /** The maximum composed scope length (in characters of its `/`-joined form)
    * before derivation is rejected. Postgres btree index entries cap at roughly
    * 2700 bytes; this guard turns the obscure database error into a clear one.
    */
  val MaxDerivedScopeLength: Int = 1800

  /** Derives a child workflow instance's scope from its parent's identity and
    * the current run context: the parent's own instance scope, the parent
    * workflow id and instance key, the parent's current run generation as an
    * `@generation` marker, and the parent's enclosing `Workflow.scoped` path.
    * Separate array positions distinguish components even when keys contain delimiters.
    *
    * @throws IllegalArgumentException
    *   when the composed scope exceeds [[MaxDerivedScopeLength]] (indicating
    *   pathologically deep child nesting).
    */
  def deriveChildScope(
      parentInstanceScope: Vector[String],
      parentWorkflowId: WorkflowId,
      parentKey: WorkflowInstanceKey,
      parentGeneration: Long,
      enclosingScopePath: Vector[String]
  ): Vector[String] = {
    val result = parentInstanceScope ++ Vector(parentWorkflowId, s"$parentKey@$parentGeneration") ++ enclosingScopePath
    if (result.mkString("/").length > MaxDerivedScopeLength)
      throw new IllegalArgumentException(
        s"Derived workflow scope exceeds $MaxDerivedScopeLength characters; " +
          "the child nesting is too deep (see ScopePath.MaxDerivedScopeLength)"
      )
    result
  }
}
