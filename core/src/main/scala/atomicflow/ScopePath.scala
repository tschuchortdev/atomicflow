package atomicflow

/** The scope of a workflow instance or of a step within an instance: an ordered
  * array of raw segment strings. `Vector.empty` is the top level. Segments are
  * compared by position, never joined, so user content never needs escaping.
  */
opaque type ScopePath = Vector[String]

object ScopePath:

  /** The maximum composed scope length (in characters of its `/`-joined form)
    * before derivation is rejected. Postgres btree index entries cap at roughly
    * 2700 bytes; this guard turns the obscure database error into a clear one. */
  val MaxDerivedScopeLength: Int = 1800

  /** The top-level scope: no enclosing segments. */
  val empty: ScopePath = Vector.empty

  /** Builds a scope from its segments. */
  def apply(segments: String*): ScopePath = segments.toVector

  /** Wraps an existing segment array, e.g. one read from storage. */
  def fromVector(segments: Vector[String]): ScopePath = segments

  extension (scope: ScopePath)
    /** The raw segment array; for storage bindings and prefix matching. */
    def segments: Vector[String] = scope
    /** This scope with one segment appended at the end. */
    def appended(segment: String): ScopePath = scope :+ segment
    /** This scope with another scope's segments appended at the end. */
    def appendedAll(suffix: ScopePath): ScopePath = scope ++ suffix
    /** The `/`-joined form, used only for length limits and diagnostics. */
    def encoded: String = scope.mkString("/")

  /** Derives a child workflow instance's scope from its parent's identity and
    * the parent's enclosing step scope: the parent's own instance scope, the
    * parent workflow id and instance key, the parent's current run generation as
    * a `key@generation` marker, and the parent's enclosing `Workflow.scoped`
    * path. Separate array positions distinguish components even when keys
    * contain delimiters.
    *
    * @throws IllegalArgumentException when the composed scope exceeds
    *   [[MaxDerivedScopeLength]] (indicating pathologically deep nesting). */
  def deriveChildScope(
      parentInstanceScope: ScopePath,
      parentWorkflowId: WorkflowId,
      parentKey: WorkflowInstanceKey,
      parentGeneration: Long,
      enclosingScopePath: ScopePath
  ): ScopePath = {
    val result =
      parentInstanceScope.segments ++ Vector(parentWorkflowId, s"$parentKey@$parentGeneration") ++ enclosingScopePath.segments
    if (result.mkString("/").length > MaxDerivedScopeLength)
      throw new IllegalArgumentException(
        s"Derived workflow scope exceeds $MaxDerivedScopeLength characters; " +
          "the child nesting is too deep (see ScopePath.MaxDerivedScopeLength)"
      )
    fromVector(result)
  }
