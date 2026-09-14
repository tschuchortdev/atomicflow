package atomicflow

import atomicflow.impl.Sha256Fingerprinter

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

/** The public step API: durable, replayed operations with per-key cache-drift
  * policies (see `spec/steps.md`).
  *
  * `atMostOnce` and the built-in retry policies arrive in later tasks.
  */
object Step {

  /** An at-least-once step: the body executes, its result is persisted after
    * completion, and on replay an unresolved `Started` record causes
    * re-execution (crash-safe for idempotent effects).
    *
    * `ensureUnchanged` values must not change between runs (`StepInputConflictException`
    * blocks the workflow if one does); `invalidateOn` values, when they change,
    * discard the previous result and re-execute; `invalidateAfter` sets a TTL
    * over the whole step.
    *
    * Values cross the serialization boundary before being returned or thrown
    * (commit-before-observation), so the first run's value is a decoded copy.
    *
    * @param key
    *   the step's stable identity within the workflow
    * @param version
    *   positive at-least-once version; bumping it creates new retryable work and
    *   stops reusing the previous version's cached result
    * @param ensureUnchanged
    *   named inputs that must be invariant between runs
    * @param invalidateOn
    *   named inputs that invalidate the cached result when they change
    * @param invalidateAfter
    *   TTL after which the cached result expires; `Duration.Inf` disables it
    */
  def atLeastOnce[A: Cacheable](
      key: String,
      version: Long = 1L,
      ensureUnchanged: Seq[StepInput[?]] = Seq.empty,
      invalidateOn: Seq[StepInput[?]] = Seq.empty,
      invalidateAfter: Duration = Duration.Inf
  )(body: => A)(using ctx: WorkflowContext, throwableCodec: Cacheable[Throwable]): A = {
    val execution = ctx.execution
    val stepId = StepId(key, execution.currentScope)
    val valueCodec = summon[Cacheable[A]]
    val fingerprints = encodeFingerprints(ensureUnchanged ++ invalidateOn)
    val now = execution.now
    val expiresAt = invalidateAfter match {
      case d: FiniteDuration => Some(now.plus(java.time.Duration.ofNanos(d.toNanos)))
      case _                 => None
    }

    def decodeFailure(payload: String): Throwable =
      try throwableCodec.read(payload)
      catch {
        case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded")
      }

    def execute(): A = {
      execution.writeStepStarted(stepId, version, "AtLeastOnce", fingerprints)
      try {
        val value = body
        val serialized =
          try valueCodec.write(value)
          catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be encoded") }
        val decoded =
          try valueCodec.read(serialized)
          catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded") }
        execution.writeStepSucceeded(stepId, version, "AtLeastOnce", fingerprints, serialized, expiresAt)
        decoded
      } catch {
        case t if isNonCacheable(t) => throw t
        case t =>
          val serialized =
            try throwableCodec.write(t)
            catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be encoded") }
          val failure =
            try throwableCodec.read(serialized)
            catch { case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded") }
          execution.writeStepFailed(stepId, version, "AtLeastOnce", fingerprints, serialized, expiresAt)
          throw failure
      }
    }

    val existing = execution.lookupStep(stepId, version).filterNot(_.expiresAt.exists(!_.isAfter(now)))

    existing match {
      case None =>
        execute()

      case Some(row) =>
        val stored = parseFingerprints(row.inputFingerprints)
        for (input <- ensureUnchanged) {
          if (stored.get(input.name) != Some(fingerprintOf(input)))
            throw new StepInputConflictException(
              s"Step '$key' input '${input.name}' changed between runs and is pinned with ensureUnchanged; refusing to re-execute"
            )
        }
        val shouldReexecute = invalidateOn.exists(input => stored.get(input.name) != Some(fingerprintOf(input)))

        if (shouldReexecute) {
          execution.deleteStep(stepId, version)
          execute()
        } else {
          row.stateKind match {
            case "succeeded" =>
              try valueCodec.read(row.statePayload)
              catch {
                case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded")
              }
            case "failed" => throw decodeFailure(row.statePayload)
            case _        => execute()
          }
        }
    }
  }

  /** Read the durable execution state of a step without executing it (no lease or
    * fence is involved). Reports the stored row even if it has expired.
    *
    * @param key
    *   the step's stable identity within the workflow
    * @param stepVersion
    *   the exact step version to inspect (`0` is the internal version for
    *   unversioned constructs)
    */
  def getExecutionState[A: Cacheable](key: String, stepVersion: Long = 0)(using
      ctx: WorkflowContext
  ): StepExecutionState[A] = {
    val stepId = StepId(key, ctx.execution.currentScope)
    val valueCodec = summon[Cacheable[A]]
    ctx.runtime.readStep(ctx.instanceId, stepId, stepVersion) match {
      case None => StepExecutionState.NeverStarted
      case Some(row) =>
        row.stateKind match {
          case "succeeded" =>
            try StepExecutionState.Completed(valueCodec.read(row.statePayload))
            catch {
              case _: Throwable => throw new StepSerializationFailed(s"Step '$key' result could not be decoded")
            }
          case "failed" =>
            val failure =
              try Cacheable.forThrowable.genericStringMessageSerializer.read(row.statePayload)
              catch {
                case _: Throwable => throw new StepSerializationFailed(s"Step '$key' failure could not be decoded")
              }
            StepExecutionState.Failed(failure)
          case _ => StepExecutionState.Started
        }
    }
  }

  /** Throwables that abort the run without durable step state: fatal JVM errors,
    * the library's control-flow exceptions (suspension, continue-as-new), lease
    * loss, and cancellation delivered at a checkpoint.
    */
  private def isNonCacheable(t: Throwable): Boolean = t match {
    case _: VirtualMachineError | _: ThreadDeath | _: LinkageError => true
    case _: WorkflowControlException                                => true
    case _: LeaseLostException                                      => true
    case _: WorkflowCancelledException                              => true
    case _                                                          => false
  }

  /** Deterministic, order-insensitive encoding of the named inputs. Each line is
    * `<hex(name)>|<base64fingerprint>`; lines are sorted by name and joined with
    * `\n`. Empty when there are no named inputs (the instance id is then the sole
    * cache key).
    */
  private def encodeFingerprints(inputs: Seq[StepInput[?]]): String =
    inputs
      .map(input => hex(input.name.getBytes(StandardCharsets.UTF_8)) + "|" + fingerprintOf(input))
      .sortBy(identity)
      .mkString("\n")

  private def parseFingerprints(encoded: String): Map[String, String] =
    if (encoded.isEmpty) Map.empty
    else
      encoded.split("\n").iterator.map { line =>
        val sep = line.indexOf('|')
        val name = new String(hexDecode(line.substring(0, sep)), StandardCharsets.UTF_8)
        name -> line.substring(sep + 1)
      }.toMap

  private def fingerprintOf(input: StepInput[?]): String =
    input.fingerprint(Sha256Fingerprinter).toString

  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString

  private def hexDecode(hex: String): Array[Byte] =
    hex.grouped(2).map(h => Integer.parseInt(h, 16).toByte).toArray
}
