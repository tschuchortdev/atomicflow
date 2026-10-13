package atomicflow

import atomicflow.internal.ScopePath
import munit.FunSuite

class ScopePathSuite extends FunSuite:

  test("deriveChildScope composes parent identity, generation marker, and enclosing path") {
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = Vector.empty,
        parentWorkflowId = "orders",
        parentKey = "order-42",
        parentGeneration = 3,
        enclosingScopePath = Vector.empty
      ),
      Vector("orders", "order-42@3")
    )
  }

  test("deriveChildScope carries the parent instance scope before the parent identity") {
    val nestedParentScope = Vector("orders", "order-42@3", "items", "poll@7")
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = nestedParentScope,
        parentWorkflowId = "worker",
        parentKey = "worker-1",
        parentGeneration = 0,
        enclosingScopePath = Vector("chunk", "retry@2")
      ),
      Vector("orders", "order-42@3", "items", "poll@7", "worker", "worker-1@0", "chunk", "retry@2")
    )
  }

  test("an empty parent scope contributes no segments") {
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = Vector.empty,
        parentWorkflowId = "orders",
        parentKey = "order-42",
        parentGeneration = 0,
        enclosingScopePath = Vector.empty
      ),
      Vector("orders", "order-42@0")
    )
  }

  test("marker formats are a trailing numeric run on the parent key and region id") {
    // the parent marker is `key@generation`; the region marker is `id@count`,
    // both raw segments with no escaping
    assertEquals(ScopePath.deriveChildScope(Vector("a"), "wf", "worker-1", 7, Vector.empty).last, "worker-1@7")
    // a key that itself ends in a marker-looking suffix stays a single segment
    val tricky = ScopePath.deriveChildScope(Vector.empty, "wf", "poll@7", 2, Vector.empty)
    assertEquals(tricky, Vector("wf", "poll@7@2"))
  }

  test("raw segments containing /, @, and \\ are preserved verbatim") {
    // injectivity is now structural (array positions), so no escaping is applied
    val crafted = Vector("x@0/B/y")
    val genuine = Vector("x@0", "B", "y@0")
    assertNotEquals(crafted, genuine)
    assertEquals(
      ScopePath.deriveChildScope(
        parentInstanceScope = Vector("a/b@c", """d\e"""),
        parentWorkflowId = "x@0/B/y",
        parentKey = "k\\p",
        parentGeneration = 1,
        enclosingScopePath = Vector("s/c/d")
      ),
      Vector("a/b@c", """d\e""", "x@0/B/y", "k\\p@1", "s/c/d")
    )
  }

  test("the length guard rejects only overlong derived scopes at the boundary") {
    // exactly at the limit composes fine: the joined scope is 1800 chars
    val atLimit = ScopePath.deriveChildScope(
      parentInstanceScope = Vector("a" * 1794),
      parentWorkflowId = "w",
      parentKey = "k",
      parentGeneration = 0,
      enclosingScopePath = Vector.empty
    )
    assertEquals(atLimit.mkString("/").length, ScopePath.MaxDerivedScopeLength)

    // one character over the limit throws with a clear message
    val e = intercept[IllegalArgumentException] {
      ScopePath.deriveChildScope(
        parentInstanceScope = Vector("a" * 1795),
        parentWorkflowId = "w",
        parentKey = "k",
        parentGeneration = 0,
        enclosingScopePath = Vector.empty
      )
    }
    assert(e.getMessage.contains("too deep"), s"unexpected message: ${e.getMessage}")
  }
