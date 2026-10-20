package atomicflow

import atomicflow.impl.Sha256Fingerprinter
import StepIdSelector.*

import scala.collection.mutable

/** A structural matcher for step identities: which step of a workflow instance
  * an operation addresses. Built with the `stepId` string interpolator and used
  * in two places:
  *
  *   - In a `StepFailed` pattern, to react to a specific step's failure:
  *     `case StepFailed(stepId"orders/$id/charge", message) => ...`
  *   - As the `restartFrom` boundary of `resetWorkflow`/`forkWorkflow`.
  *
  * A selector describes a path of scope segments ending in the step key, the
  * same shape `Workflow.scoped`, `Workflow.loop`, and races build at run time:
  * the last segment matches the step key, the preceding segments match the
  * enclosing scopes.
  *
  * Patterns are written as `/`-separated segments. Without a leading `/`, the
  * pattern matches any path *ending* with the given segments, so enclosing
  * scopes further up never need to be spelled out; a leading `/` anchors the
  * pattern at the instance root and requires the full path. `*` matches any
  * text within one segment, `**` matches zero or more whole segments. An
  * interpolation hole `$x` is a literal segment in expression position (a
  * `String` is used as the segment text, any other `Fingerprintable` value as
  * its fingerprint, mirroring `Workflow.scoped`) and a binding for the matched
  * segment text in pattern position. Iteration markers (`name@3`, as written by
  * `Workflow.loop`) are ignored on both sides: `orders` also matches
  * `orders@3`, and iteration counts cannot be pinned. Keys created through the
  * workflow API never contain `/`, `*`, `@`, `[`, `]`, `{`, `}`, or a leading
  * `_`, so these characters are free for pattern syntax.
  *
  * Matching is a pure function of the selector and the [[StepId]], so it gives
  * the same answer on the first run and on every replay.
  */
final class StepIdSelector private(
    anchored: Boolean,
    segmentPatterns: Vector[StepIdSelector.SegmentPattern]
) {

  /** Whether this selector matches `step`. */
  def matches(stepId: StepId): Boolean =
    matchesPath(stepId.fullPathWithoutIterations).isDefined

  /** The pattern text, parseable by `StepSelector.apply`. */
  override def toString: String =
    (if (anchored) "/" else "") + segmentPatterns.map(StepIdSelector.renderSegment).mkString("/")

  private[atomicflow] def matchesPath(path: Vector[String]): Option[Vector[String]] = {
    val starts = if (anchored) Vector(0) else (0 to path.length).toVector
    starts.iterator
      .flatMap(start => StepIdSelector.matchSegments(segmentPatterns.toList, path.drop(start).toList, Vector.empty))
      .nextOption()
  }
}

object StepIdSelector {

  /** Parses a pattern string: `/`-separated segments with `*` and `**`
    * wildcards, a leading `/` anchoring at the instance root. Iteration
    * markers are ignored during matching, so they cannot be pinned.
    */
  def apply(patternStr: String): StepIdSelector = {
    val (anchored, segments) = parseChunks(Vector(patternStr), holeCount = 0)
    new StepIdSelector(anchored, segments)
  }

  /** One segment of a pattern: either `**` (any number of segments) or the
    * atoms of one segment, where `*` and interpolation holes match any text.
    */
  private enum SegmentPattern {
    case AnySegments
    case Within(atoms: Vector[Atom])
  }

  private enum Atom {
    private[StepIdSelector] case Text(value: String)
    private[StepIdSelector] case AnyRun
    private[StepIdSelector] case Hole(index: Int)
  }

  /** The interpolator object behind `stepId"..."`.
    *
    * In expression position the holes are values: a `String` becomes a literal
    * segment, any other `Fingerprintable` value its fingerprint segment.
    * `stepId"orders/$order/charge"` is the selector for the step `charge`
    * inside the `scoped(order)` scope under `orders`. All holes must be of the
    * same kind: `String` segments, or `Fingerprintable` values of one type.
    *
    * In pattern position the holes bind the matched segment text:
    * `case StepFailed(stepId"orders/$id/charge", message) => ...`.
    */
  final class Interpolator(context: StringContext) {
    private val chunks: Vector[String] = context.parts.toVector
    private val holeCount: Int = chunks.length - 1

    /** Builds a selector with no interpolation holes. */
    def apply(): StepIdSelector = build(Vector.empty)

    /** Builds a selector whose holes are the literal segments `names`. */
    def apply(names: String*): StepIdSelector = {
      names.foreach(name => ScopePath.requireValidKey("interpolated scope segment", name))
      build(names.toVector)
    }

    /** Builds a selector whose holes are the values' fingerprint segments, the
      * same segments `Workflow.scoped(elem)` derives.
      */
    def apply[A: Fingerprintable](value: A, values: A*): StepIdSelector =
      build((value +: values).toVector.map(value => Fingerprintable[A].fingerprint(value, Sha256Fingerprinter).toString))

    private def build(holeValues: Vector[String]): StepIdSelector = {
      require(
        holeValues.length == holeCount,
        s"the pattern has $holeCount interpolation hole(s) but ${holeValues.length} value(s) were given"
      )
      val text = chunks.zipAll(holeValues.map(identity), "", "").map((chunk, value) => chunk + value).mkString
      val (anchored, segments) = parseChunks(Vector(text), holeCount = 0)
      new StepIdSelector(anchored, segments)
    }

    /** Pattern position: matches a [[StepId]] and binds the hole segments. */
    def unapplySeq(x: Matchable): Option[Seq[String]] = x match {
      case stepId: StepId =>
        val (anchored, segments) = parseChunks(chunks, holeCount)
        val path = stepId.fullPathWithoutIterations
        val starts = if (anchored) Vector(0) else (0 to path.length).toVector
        starts.iterator
          .flatMap(start => matchSegments(segments.toList, path.drop(start).toList, Vector.empty))
          .nextOption()
          .map(_.toSeq)
      case _ => None
    }
  }

  private val ForbiddenPatternChars = "[]{}"

  private type Token = Either[Char, Int]

  /** Splits pattern text into segments. `chunks` are the literal pieces between
    * `holeCount` interpolation holes; each hole matches any text within its
    * segment (a whole segment when the adjacent text is empty).
    */
  private def parseChunks(chunks: Vector[String], holeCount: Int): (Boolean, Vector[SegmentPattern]) = {
    require(chunks.length == holeCount + 1, "inconsistent pattern chunks")
    val tokens: Vector[Token] = chunks.zipWithIndex.flatMap { (chunk, index) =>
      val chars: Vector[Token] = chunk.map(char => Left(char): Token).toVector
      if (index < holeCount) chars :+ Right(index) else chars
    }
    val anchored = tokens.headOption.contains(Left('/'))
    val body = if (anchored) tokens.drop(1) else tokens

    val groups = mutable.ListBuffer[Vector[Token]]()
    var current = Vector.empty[Token]
    body.foreach {
      case Left('/') =>
        groups += current
        current = Vector.empty
      case token =>
        current :+= token
    }
    groups += current

    val segmentGroups = groups.toVector
    require(
      segmentGroups.forall(_.nonEmpty),
      "a step selector pattern must not contain empty segments"
    )
    val segments = segmentGroups.map {
      case Vector(Left('*'), Left('*')) => SegmentPattern.AnySegments
      case group                        => SegmentPattern.Within(atomsOf(group))
    }
    (anchored, segments)
  }

  /** Builds one segment's atoms. A trailing iteration marker (a literal
    * `@<digits>` at the segment's end) is removed, so `poll@2` and even
    * `poll-x@2` match every iteration exactly like `poll` and `poll-*`;
    * iteration counts cannot be pinned.
    */
  private def atomsOf(group0: Vector[Token]): Vector[Atom] = {
    var group = group0
    var stripped = true
    while (stripped) {
      stripped = false
      val trailingLiterals = group.reverse.takeWhile {
        case Left(_) => true
        case _       => false
      }
      val trailing = trailingLiterals.reverse.collect { case Left(char) => char }.mkString
      val at = trailing.lastIndexOf('@')
      if (at >= 0 && trailing.length > at + 1 && trailing.substring(at + 1).forall(_.isDigit)) {
        group = group.dropRight(trailing.length - at)
        stripped = true
      }
    }
    require(group.nonEmpty, "a step selector pattern must not contain empty segments")
    group.map(atomOf)
  }

  private def atomOf(token: Token): Atom = token match {
    case Left('/') => throw new IllegalStateException("unreachable: '/' is split on before")
    case Left('*') => Atom.AnyRun
    case Left(char) =>
      require(!ForbiddenPatternChars.contains(char), s"a step selector pattern must not contain '$char'")
      Atom.Text(char.toString)
    case Right(index) => Atom.Hole(index)
  }

  private def renderSegment(pattern: SegmentPattern): String = pattern match {
    case SegmentPattern.AnySegments => "**"
    case SegmentPattern.Within(atoms) =>
      atoms.map {
        case Atom.Text(value) => value
        case Atom.AnyRun      => "*"
        case _: Atom.Hole     => "*"
      }.mkString
  }

  extension (stepId: StepId)
    /** Scope segments plus the key, with iteration markers (`name@3`) removed
      * so patterns need not spell them out.
      */
    private def fullPathWithoutIterations: Vector[String] =
      (stepId.scope.segments :+ stepId.key).map(stripIterationMarker)

  /** Repeatedly removes a trailing `@<digits>` marker. */
  private def stripIterationMarker(segment: String): String = {
    var current = segment
    var stripping = true
    while (stripping) {
      val at = current.lastIndexOf('@')
      if (at > 0 && current.length > at + 1 && current.substring(at + 1).forall(_.isDigit))
        current = current.substring(0, at)
      else
        stripping = false
    }
    current
  }

  /** Matches `patterns` against the head of `segments`, returning the hole
    * bindings in order. `AnySegments` matches zero or more segments. Failed
    * (pattern, segment) positions are memoized, so repeated stars stay linear.
    */
  private def matchSegments(
      patterns: List[SegmentPattern],
      segments: List[String],
      bindings: Vector[String]
  ): Option[Vector[String]] = {
    val failed = mutable.HashSet.empty[(Int, Int)]
    def go(patterns: List[SegmentPattern], segments: List[String], bindings: Vector[String]): Option[Vector[String]] = {
      val state = (patterns.length, segments.length)
      (patterns, segments) match {
        case (Nil, Nil) => Some(bindings)
        case _ if failed.contains(state) => None
        case (Nil, _) => None
        case (SegmentPattern.AnySegments :: rest, ss) =>
          val result = go(rest, ss, bindings).orElse(ss match {
            case Nil    => None
            case _ :: _ => go(patterns, segments.tail, bindings)
          })
          if (result.isEmpty) failed.add(state)
          result
        case (SegmentPattern.Within(atoms) :: rest, segment :: tail) =>
          val result = matchSegment(atoms, segment).flatMap(binding =>
            go(rest, tail, bindings ++ binding))
          if (result.isEmpty) failed.add(state)
          result
        case (SegmentPattern.Within(_) :: _, Nil) => None
      }
    }
    go(patterns, segments, bindings)
  }

  /** Matches one segment's atoms, returning the matched hole texts. Stars and
    * holes match any (possibly empty) run of characters; backtracking prefers
    * longest matches. Failed (atom, character) positions are memoized, so
    * repeated stars stay linear.
    */
  private def matchSegment(atoms: Vector[Atom], segment: String): Option[Vector[String]] = {
    val failed = mutable.HashSet.empty[(Int, Int)]
    def go(atomIndex: Int, charIndex: Int, bindings: Vector[String]): Option[Vector[String]] = {
      if (atomIndex == atoms.length)
        Option.when(charIndex == segment.length)(bindings)
      else if (failed.contains((atomIndex, charIndex)))
        None
      else {
        val result = atoms(atomIndex) match {
          case Atom.Text(value) =>
            if (segment.startsWith(value, charIndex))
              go(atomIndex + 1, charIndex + value.length, bindings)
            else
              None
          case Atom.AnyRun =>
            (charIndex to segment.length).reverseIterator
              .flatMap(end => go(atomIndex + 1, end, bindings))
              .nextOption()
          case _: Atom.Hole =>
            (charIndex to segment.length).reverseIterator
              .flatMap(end => go(atomIndex + 1, end, bindings :+ segment.substring(charIndex, end)))
              .nextOption()
        }
        if (result.isEmpty) failed.add((atomIndex, charIndex))
        result
      }
    }
    go(0, 0, Vector.empty)
  }
}

/** Builds a [[StepIdSelector]] from an interpolated string. See
  * [[StepIdSelector.Interpolator]].
  */
extension (context: StringContext) {
  def stepId: StepIdSelector.Interpolator = StepIdSelector.Interpolator(context)
}
