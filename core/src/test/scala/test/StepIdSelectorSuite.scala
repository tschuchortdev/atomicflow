package test

import atomicflow.{ScopePath, StepId, StepIdSelector, stepId}
import atomicflow.impl.Sha256Fingerprinter
import munit.FunSuite

class StepIdSelectorSuite extends FunSuite {

  private def id(key: String, segments: String*): StepId = {
    StepId(key, ScopePath(segments*))
  }

  private def assertMatches(selector: StepIdSelector, step: StepId): Unit = {
    assertEquals(selector.matches(step), true, clue = s"${selector.toString} must match ${step.pathStr}")
  }

  private def assertNoMatch(selector: StepIdSelector, step: StepId): Unit = {
    assertEquals(selector.matches(step), false, clue = s"${selector.toString} must not match ${step.pathStr}")
  }

  test("expression position without holes builds a suffix matcher") {
    assertMatches(stepId"charge", id("charge", "orders", "order-1"))
    assertNoMatch(stepId"charge", id("other", "orders", "order-1"))
  }

  test("a multi-segment pattern matches the path ending and nothing shorter") {
    assertMatches(stepId"orders/charge", id("charge", "orders"))
    assertMatches(stepId"orders/charge", id("charge", "a", "orders"))
    assertNoMatch(stepId"orders/charge", id("charge"))
    assertNoMatch(stepId"orders/charge", id("charge", "orders", "extra"))
    assertNoMatch(stepId"orders/charge", id("charge", "shipping"))
  }

  test("a leading slash anchors the pattern at the instance root") {
    assertMatches(stepId"/orders/charge", id("charge", "orders"))
    assertNoMatch(stepId"/orders/charge", id("charge", "a", "orders"))
  }

  test("a star matches exactly one whole segment") {
    assertMatches(stepId"orders/*/charge", id("charge", "orders", "fp1"))
    assertNoMatch(stepId"orders/*/charge", id("charge", "orders"))
    assertNoMatch(stepId"orders/*/charge", id("charge", "orders", "fp1", "deep"))
    assertMatches(stepId"*/charge", id("charge", "x"))
  }

  test("two stars match zero or more segments") {
    assertMatches(stepId"poll/**/fetch", id("fetch", "poll"))
    assertMatches(stepId"poll/**/fetch", id("fetch", "poll", "a"))
    assertMatches(stepId"poll/**/fetch", id("fetch", "poll", "a", "b"))
    assertNoMatch(stepId"poll/**/fetch", id("fetch", "other"))
    assertMatches(stepId"poll/**/fetch", id("fetch", "a", "poll"))
  }

  test("a star matches text within a segment") {
    assertMatches(stepId"foo/bar-*", id("bar-a", "foo"))
    assertMatches(stepId"foo/bar-*", id("bar-b", "foo"))
    assertNoMatch(stepId"foo/bar-*", id("bop", "foo", "bar-c"))
    assertNoMatch(stepId"foo/bar-*", id("bar", "foo"))
    assertMatches(stepId"bar-*", id("bar-anything"))
    assertMatches(stepId"*-x", id("prefix-x"))
  }

  test("iteration markers are ignored on both sides and cannot be pinned") {
    assertMatches(stepId"poll/fetch", id("fetch", "poll@2"))
    assertMatches(stepId"poll@2/fetch", id("fetch", "poll@7"))
    assertMatches(stepId"poll", id("poll@2"))
    assertMatches(stepId"poll@2", id("poll"))
  }

  test("a hole in pattern position binds the matched segment") {
    val interpolator = StepIdSelector.Interpolator(StringContext("orders/", "/charge"))
    assertEquals(interpolator.unapplySeq(id("charge", "orders", "fp-1")), Some(Seq("fp-1")))
    assertEquals(interpolator.unapplySeq(id("charge", "orders", "fp-2")), Some(Seq("fp-2")))
    assertEquals(interpolator.unapplySeq(id("charge", "shipping", "fp-1")), None)
    assertEquals(interpolator.unapplySeq(id("charge", "orders")), None)
  }

  test("a hole matches the whole segment only") {
    val whole = StepIdSelector.Interpolator(StringContext("orders/", ""))
    assertEquals(whole.unapplySeq(id("order-1", "orders")), Some(Seq("order-1")))
    // a hole adjacent to text binds the remainder of that segment
    val partial = StepIdSelector.Interpolator(StringContext("item-", ""))
    assertEquals(partial.unapplySeq(id("item-7", "orders")), Some(Seq("7")))
    assertEquals(partial.unapplySeq(id("other-7", "orders")), None)
  }

  test("a String hole in expression position is a literal segment") {
    val order = "order-42"
    assertMatches(stepId"orders/$order/charge", id("charge", "orders", "order-42"))
    assertNoMatch(stepId"orders/$order/charge", id("charge", "orders", "order-43"))
  }

  test("an interpolated String must satisfy the key rules") {
    val wildcard = "a*b"
    val separator = "a/b"
    val marker = "a@3"
    intercept[IllegalArgumentException](stepId"orders/$wildcard/charge")
    intercept[IllegalArgumentException](stepId"orders/$separator/charge")
    intercept[IllegalArgumentException](stepId"orders/$marker/charge")
  }

  test("an Integer hole in expression position is the value's fingerprint segment") {
    val page = 7
    val selector = stepId"page/$page/charge"
    val fingerprint = Sha256Fingerprinter.fingerprint(page).toString
    assertMatches(selector, id("charge", "page", fingerprint))
    assertNoMatch(selector, id("charge", "page", "other"))
  }

  test("an interpolated fingerprint renders without '/' so selectors round-trip") {
    val value = 12345
    val fingerprint = Sha256Fingerprinter.fingerprint(value).toString
    val rendered = stepId"orders/$value/charge".toString
    assertEquals(rendered, s"orders/$fingerprint/charge")
    val reparsed = StepIdSelector(rendered)
    assertMatches(reparsed, id("charge", "orders", fingerprint))
    assertNoMatch(reparsed, id("charge", "orders", "12345"))
  }

  test("an interpolated String hole is a literal segment, not a fingerprint") {
    val value = "order-42"
    val selector = stepId"orders/$value/charge"
    assertMatches(selector, id("charge", "orders", "order-42"))
    assertNoMatch(selector, id("charge", "orders", Sha256Fingerprinter.fingerprint(value).toString))
  }

  test("several fingerprint holes of one type build one selector") {
    val customer = 42
    val item = 7
    val selector = stepId"orders/$customer/items/$item/charge"
    assertMatches(selector, id("charge", "orders", Sha256Fingerprinter.fingerprint(42).toString, "items", Sha256Fingerprinter.fingerprint(7).toString))
    assertNoMatch(selector, id("charge", "orders", Sha256Fingerprinter.fingerprint(42).toString, "other", Sha256Fingerprinter.fingerprint(7).toString))
  }

  test("a fingerprint segment may start with '_'") {
    // find a value whose base64url fingerprint starts with '_' (1/64 of values)
    val underscoreValue = Iterator.from(0).map(_.toLong).find(value =>
      Sha256Fingerprinter.fingerprint(value).toString.startsWith("_")).get
    val fingerprint = Sha256Fingerprinter.fingerprint(underscoreValue).toString
    assert(fingerprint.startsWith("_"), s"test setup: expected a leading-underscore fingerprint, got $fingerprint")
    // scoped(elem) accepts it
    val selector = stepId"orders/$underscoreValue/charge"
    assertMatches(selector, id("charge", "orders", fingerprint))
  }

  test("an anchored full-path pattern normalizes iteration markers like matching does") {
    val step = id("charge", "poll@7@2")
    val selector = stepId"/poll/charge"
    assertMatches(selector, step)
    assertMatches(selector, id("charge", "poll@9"))
    assertNoMatch(selector, id("charge", "poll", "deep"))
    // render equivalence: the rendered text builds the same selector
    assertMatches(StepIdSelector(selector.toString), step)
  }

  test("iteration markers are stripped from patterns with wildcards and holes") {
    assertMatches(stepId"poll*@2/charge", id("charge", "poll-x@7"))
    assertNoMatch(stepId"poll*@2/charge", id("charge", "other-x@7"))
    // the compiler hands the interpolator "poll-" and "@2/charge" around the hole
    val holePattern = StepIdSelector.Interpolator(StringContext("poll-", "@2/charge"))
    assertEquals(holePattern.unapplySeq(id("charge", "poll-x@7")), Some(Seq("x")))
    assertEquals(holePattern.unapplySeq(id("charge", "poll-y@8")), Some(Seq("y")))
    assertEquals(holePattern.unapplySeq(id("charge", "other-x@7")), None)
  }

  test("repeated stars stay linear when matching fails") {
    val deepSelector = StepIdSelector("/" + "**/" * 24 + "b")
    val deepStep = StepId("c", ScopePath.fromVector(Vector.fill(24)("a")))
    val startedAt = System.nanoTime()
    assertEquals(deepSelector.matches(deepStep), false)
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000
    assert(elapsedMillis < 5000, s"matching degenerated to $elapsedMillis ms")
  }

  test("render is parseable and preserves the anchoring") {
    assertEquals(stepId"orders/charge".toString, "orders/charge")
    assertEquals(stepId"/orders/charge".toString, "/orders/charge")
    assertEquals(stepId"poll/**/fetch".toString, "poll/**/fetch")
    assertEquals(stepId"foo/bar-*".toString, "foo/bar-*")
    Seq("orders/charge", "/orders/charge", "poll/**/fetch", "foo/bar-*").foreach { rendered =>
      assertEquals(StepIdSelector(rendered).toString, rendered)
    }
  }

  test("parse accepts the same grammar as the interpolator") {
    assertMatches(StepIdSelector("orders/**/charge"), id("charge", "orders", "a", "b"))
    assertNoMatch(StepIdSelector("/orders/**/charge"), id("charge", "x", "orders", "a"))
  }

  test("an anchored full-path pattern is the exact selector for a step id") {
    val selector = stepId"/orders/charge"
    assertMatches(selector, id("charge", "orders"))
    assertNoMatch(selector, id("charge", "orders", "sub"))
    assertNoMatch(selector, id("charge"))
    assertEquals(selector.toString, "/orders/charge")
  }

  test("an anchored full-path pattern matches a loop iteration of the same scope") {
    assertMatches(stepId"/orders/charge", id("charge", "orders@3"))
  }

  test("invalid patterns are rejected") {
    intercept[IllegalArgumentException](StepIdSelector("a//b"))
    intercept[IllegalArgumentException](StepIdSelector("a/"))
    intercept[IllegalArgumentException](StepIdSelector(""))
    intercept[IllegalArgumentException](StepIdSelector("a/[b]"))
  }

  test("the interpolator rejects an empty segment between slashes") {
    val interpolator = StepIdSelector.Interpolator(StringContext("a/", "/b"))
    intercept[IllegalArgumentException](interpolator())
  }

  test("the interpolator rejects a hole count mismatch in expression position") {
    intercept[IllegalArgumentException](StepIdSelector.Interpolator(StringContext("orders/", "/charge")).apply())
  }
}
