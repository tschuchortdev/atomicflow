package atomicflow

import atomicflow.internal.ScopePath
import munit.FunSuite

class ScopePathSuite extends FunSuite:

  test("escaping protects the structural delimiters") {
    assertEquals(ScopePath.escapeScopeSegment("plain"), "plain")
    assertEquals(ScopePath.escapeScopeSegment("""a\b"""), """a\\b""")
    assertEquals(ScopePath.escapeScopeSegment("a/b"), """a\/b""")
    assertEquals(ScopePath.escapeScopeSegment("a@b"), """a\@b""")
    // the example from the spec: a crafted segment cannot fake a chain
    assertEquals(ScopePath.escapeScopeSegment("x@0/B/y"), """x\@0\/B\/y""")
  }

  test("escaped segments cannot collide with genuine chains") {
    // spec: `x@0/B/y` encodes to `x\@0\/B\/y`, which cannot collide with the
    // genuine chain composed of segments "x@0", "B", "y@0"
    val crafted = ScopePath.escapeScopeSegment("x@0/B/y")
    val genuine = ScopePath.joinEscaped(
      ScopePath.escapeScopeSegment("x@0"),
      ScopePath.escapeScopeSegment("B"),
      ScopePath.escapeScopeSegment("y@0")
    )
    assertNotEquals(crafted, genuine)
  }

  test("joinEscaped composes escaped segments with the / delimiter") {
    assertEquals(ScopePath.joinEscaped("a", "b", "c"), "a/b/c")
    assertEquals(ScopePath.joinEscaped("a"), "a")
  }

  test("count markers are appended after the escaped key") {
    val escapedKey = ScopePath.escapeScopeSegment("worker-1")
    assertEquals(escapedKey + "@0", "worker-1@0")
    // a key containing a marker-looking suffix stays unambiguous
    val tricky = ScopePath.escapeScopeSegment("poll@7")
    assertEquals(tricky + "@2", """poll\@7@2""")
  }

  test("shortening replaces only overlong parent portions with a marked stable hash") {
    val short = "a" * (ScopePath.MaxEncodedLength - 1)
    assertEquals(ScopePath.shortenIfTooLong(short), short)

    val long1 = "a" * (ScopePath.MaxEncodedLength + 10)
    val long2 = "b" * (ScopePath.MaxEncodedLength + 10)
    val shortened1 = ScopePath.shortenIfTooLong(long1)
    // deterministic, marked, sha256-hex
    assertEquals(shortened1, ScopePath.shortenIfTooLong(long1))
    assert(clue(shortened1).startsWith("shortened[sha256:"), s"unexpected: $shortened1")
    assert(clue(shortened1).endsWith("]"))
    val hash = shortened1.stripPrefix("shortened[sha256:").stripSuffix("]")
    assertEquals(hash.length, 64)
    assert(hash.forall(c => c.isDigit || ('a' to 'f').contains(c)))
    // different overlong scopes shorten differently
    assertNotEquals(shortened1, ScopePath.shortenIfTooLong(long2))
    // exactly at the limit is kept
    val exact = "c" * ScopePath.MaxEncodedLength
    assertEquals(ScopePath.shortenIfTooLong(exact), exact)
  }
