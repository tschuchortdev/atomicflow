package atomicflow

import atomicflow.Fingerprintable.Fingerprint
import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.annotation.implicitNotFound

@implicitNotFound("Cannot be used outside a Step definition: `Step(...) {  }`\nYou can require a StepContext for the enclosing method by adding a using clause `(using StepContext)` to its definition.")
trait StepContext[Out] {
  def meta: StepMeta

  def workflowCtx: WorkflowContext[?, ?]

  override lazy val toString: String = s"$workflowCtx/step:${meta.id}${meta.name.fold("")(name => "#" + URLEncoder.encode(name, StandardCharsets.UTF_8))}"

  def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints

  def idempotencyStore: StepIdempotencyStore

  def cache(using Cacheable[Out]): StepCache[Out]

  def onComplete(f: Out => Unit): Unit

  def onCompensate(f: => Unit): Unit
}
