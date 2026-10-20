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

  /** Characters forbidden in user-chosen keys. They are the pattern syntax of
    * [[StepIdSelector]] (segment separator, wildcards, iteration marker,
    * reserved brackets), so a key containing them could never be addressed by
    * a selector.
    */
  private val ForbiddenKeyChars = "/*@[]{}"

  /** Checks a user-chosen key that becomes a scope-path segment or a step key:
    * step keys, `Workflow.scoped` keys, region ids, and race branch keys.
    * Fails with [[IllegalArgumentException]] when the key is empty, contains a
    * forbidden character, or starts with `_` (reserved for engine-internal
    * keys).
    */
  def requireValidKey(what: String, key: String): Unit = {
    require(key.nonEmpty, s"$what must not be empty")
    val forbidden = key.find(ForbiddenKeyChars.contains)
    require(
      forbidden.isEmpty,
      s"$what '$key' must not contain '${forbidden.get}'; the characters / * @ [ ] { } are reserved for step selectors"
    )
    require(
      !key.startsWith("_"),
      s"$what '$key' must not start with '_'; leading underscores are reserved for engine-internal keys"
    )
  }

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
