package atomicflow.internal

import munit.FunSuite

class FramingSuite extends FunSuite {

  test("write produces a length-prefixed frame") {
    assertEquals(Framing.write("abc"), "3:abc")
    assertEquals(Framing.write(""), "0:")
  }

  test("read returns the content and the offset just after the frame") {
    assertEquals(Framing.read("3:abc", 0), ("abc", 5))
    assertEquals(Framing.read("0:", 0), ("", 2))
  }

  test("read handles content containing colons") {
    assertEquals(Framing.read(Framing.write("a:b:c"), 0), ("a:b:c", 7))
  }

  test("concatenated frames read independently at their offsets") {
    val framed = Framing.write("a") + Framing.write("bc") + Framing.write("")
    assertEquals(Framing.read(framed, 0), ("a", 3))
    assertEquals(Framing.read(framed, 3), ("bc", 7))
    assertEquals(Framing.read(framed, 7), ("", 9))
  }

  test("frame length counts UTF-16 chars, so non-BMP content round-trips") {
    val emoji = "\uD83D\uDE00"
    assertEquals(Framing.write(emoji), s"2:$emoji")
    assertEquals(Framing.read(Framing.write(emoji), 0), (emoji, 4))
  }

  test("read rejects offsets outside the string") {
    val e1 = intercept[IllegalArgumentException](Framing.read("1:a", -1))
    assert(e1.getMessage.contains("no frame at offset -1"))
    val e2 = intercept[IllegalArgumentException](Framing.read("1:a", 3))
    assert(e2.getMessage.contains("no frame at offset 3"))
    val e3 = intercept[IllegalArgumentException](Framing.read("", 0))
    assert(e3.getMessage.contains("no frame at offset 0"))
  }

  test("read rejects a missing or empty length") {
    val noColon = intercept[IllegalArgumentException](Framing.read("abc", 0))
    assert(noColon.getMessage.contains("missing or empty length"))
    val empty = intercept[IllegalArgumentException](Framing.read(":abc", 0))
    assert(empty.getMessage.contains("missing or empty length"))
  }

  test("read rejects a non-numeric length") {
    val e = intercept[IllegalArgumentException](Framing.read("1x:abc", 0))
    assert(e.getMessage.contains("invalid frame length '1x'"))
  }

  test("read rejects a length that overflows Int") {
    val e = intercept[IllegalArgumentException](Framing.read("99999999999999999999:a", 0))
    assert(e.getMessage.contains("frame length overflow"))
  }

  test("read rejects content that overruns the string") {
    val e = intercept[IllegalArgumentException](Framing.read("5:abc", 0))
    assert(e.getMessage.contains("overruns the string"))
  }

  test("read and write are inverse for arbitrary content") {
    val samples = List("", "x", "a:b", "12:34", "héllo wörld", "{\"a\":[1,2]}")
    samples.foreach(s => assertEquals(Framing.read(Framing.write(s), 0)._1, s))
  }
}
