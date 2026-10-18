package atomicflow

import munit.FunSuite

class ScopePathSuite extends FunSuite:

  test("deriveChildScope composes parent identity, generation marker, and enclosing path") {
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = ScopePath.empty,
        parentWorkflowId = "orders",
        parentKey = "order-42",
        parentGeneration = 3,
        enclosingScopePath = ScopePath.empty
      ),
      ScopePath("orders", "order-42@3")
    )
  }

  test("deriveChildScope carries the parent instance scope before the parent identity") {
    val nestedParentScope = ScopePath("orders", "order-42@3", "items", "poll@7")
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = nestedParentScope,
        parentWorkflowId = "worker",
        parentKey = "worker-1",
        parentGeneration = 0,
        enclosingScopePath = ScopePath("chunk", "retry@2")
      ),
      ScopePath("orders", "order-42@3", "items", "poll@7", "worker", "worker-1@0", "chunk", "retry@2")
    )
  }

  test("an empty parent scope contributes no segments") {
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = ScopePath.empty,
        parentWorkflowId = "orders",
        parentKey = "order-42",
        parentGeneration = 0,
        enclosingScopePath = ScopePath.empty
      ),
      ScopePath("orders", "order-42@0")
    )
  }

  test("marker formats are a trailing numeric run on the parent key and region id") {
    // the parent marker is `key@generation`; the region marker is `id@count`,
    // both raw segments with no escaping
    assertEquals(ScopePath.deriveChildScope(ScopePath("a"), "wf", "worker-1", 7, ScopePath.empty).segments.last, "worker-1@7")
    // a key that itself ends in a marker-looking suffix stays a single segment
    val tricky = ScopePath.deriveChildScope(ScopePath.empty, "wf", "poll@7", 2, ScopePath.empty)
    assertEquals(tricky, ScopePath("wf", "poll@7@2"))
  }

  test("raw segments containing /, @, and \\ are preserved verbatim") {
    // injectivity is now structural (array positions), so no escaping is applied
    val crafted = ScopePath("x@0/B/y")
    val genuine = ScopePath("x@0", "B", "y@0")
    assertNotEquals(crafted, genuine)
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = ScopePath("a/b@c", """d\e"""),
        parentWorkflowId = "x@0/B/y",
        parentKey = "k\\p",
        parentGeneration = 1,
        enclosingScopePath = ScopePath("s/c/d")
      ),
      ScopePath("a/b@c", """d\e""", "x@0/B/y", "k\\p@1", "s/c/d")
    )
  }

  test("the length guard rejects only overlong derived scopes at the boundary") {
    // exactly at the limit composes fine: the joined scope is 1800 chars
    val atLimit = ScopePath.deriveChildScope(
      parentInstanceScope = ScopePath("a" * 1794),
      parentWorkflowId = "w",
      parentKey = "k",
      parentGeneration = 0,
      enclosingScopePath = ScopePath.empty
    )
    assertEquals(atLimit.encoded.length, ScopePath.MaxDerivedScopeLength)

    // one character over the limit throws with a clear message
    val e = intercept[IllegalArgumentException] {
      ScopePath.deriveChildScope(
        parentInstanceScope = ScopePath("a" * 1795),
        parentWorkflowId = "w",
        parentKey = "k",
        parentGeneration = 0,
        enclosingScopePath = ScopePath.empty
      )
    }
    assert(e.getMessage.contains("too deep"), s"unexpected message: ${e.getMessage}")
  }
