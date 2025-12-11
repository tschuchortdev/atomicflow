package atomicflow

import atomicflow.Workflow.*
import atomicflow.WorkflowContext.given_WorkflowInstanceMeta 
import atomicflow.WorkflowRuntime.{WorkflowStoppedToAwaitManyConditions, WorkflowStoppedToWait}

import java.time.{Clock, Instant}
import java.util.UUID
import scala.util.control.ControlThrowable

case class Workflow[In: Cacheable, Out] private(
                                                 meta: WorkflowMeta,
                                                 body: (WorkflowContext, In) => Out
                                               ) {
  def newInstance(instanceId: String)(using runtime: WorkflowRuntime): WorkflowInstanceBuilder[In, Out] =

}

object Workflow {
  def apply[In: Cacheable, Out](
                                 id: WorkflowId,
                                 name: String,
                                 description: String | Unit = ()
                               )(
                                 body: In => WorkflowContext ?=> Out
                               ): Workflow[In, Out] = {
    val workflowMeta = WorkflowMeta(
      workflowId = id,
      workflowName = name,
      workflowDescription = description match {
        case () => None
        case string: String => Some(string)
      }
    )

    new Workflow[In, Out](
      meta = workflowMeta,
      body = { (ctx: WorkflowContext, in: In) =>
        body(in)(using ctx)
      }
    )
  }

  /*def apply[Out](
                  id: WorkflowId,
                  name: String,
                  description: String | Unit = ()
                )(
                  body: () => WorkflowContext[Unit, Out] ?=> Out
                ): Workflow[Unit, Out] =
    apply[Unit, Out](id, name, description)(_ => body())*/

  def meta(using ctx: WorkflowContext): WorkflowInstanceMeta = ctx.workflowInstanceMeta

  def instanceId(using ctx: WorkflowContext): WorkflowInstanceKey = ctx.workflowInstanceMeta.workflowInstanceKey
  
  def workflowId(using ctx: WorkflowContext): WorkflowId = ctx.workflowInstanceMeta.workflowId

  def stopAndAwaitTimer[In, Out](restartAfter: Instant)(using clk: Clock)(using ctx: WorkflowContext): Nothing | Unit =
    if (restartAfter.isAfter(Instant.now(clk)))
      throw WorkflowRuntime.WorkflowStoppedToAwaitTimer(restartAfter)
    else ()

  def stopAndAwaitSignal[S, In, Out](signal: Signal[S])(using ctx: WorkflowContext): Nothing | S =
    ctx.workflowRuntime.getSignalStore.getSignalValue(signal) match {
      case Some(value) => value
      case None => throw WorkflowRuntime.WorkflowStoppedToAwaitSignal(signal)
    }

  def stopAndAwaitWorkflow[Out](workflow: Workflow[?, Out], instance: WorkflowInstanceKey)(using ctx: WorkflowContext): Nothing | Out =
    ctx.workflowRuntime.scheduleWakeupOnWorkflowCompletion(ctx.workflowInstanceMeta.workflowInstanceKey, workflow.meta.workflowId)

  def subworkflow[Res](key: String)(using ctx: WorkflowContext)(
    body: WorkflowContext ?=> Res
  ): Either[WorkflowStoppedToWait, Res] =
    try { Right(body(using ctx.withSubworkflowScope(key))) }
    catch { case e: WorkflowStoppedToWait =>
      Left(e)
    }

  def subworkflowForEach[A, Res](elems: Iterable[A])(parallelism: Int = 1)(
    subworkflowKey: A => String
  )(processElem: A => WorkflowContext ?=> Res
  )(using outerCtx: WorkflowContext): Vector[Res] = {

    val resultsOrStops: Vector[Either[WorkflowStoppedToWait, Res]] = ox.mapPar(elems)(parallelism) { a =>
      subworkflow(subworkflowKey(a))(using outerCtx) { innerCtx ?=> processElem(a)(using innerCtx) }
    }.toVector

    val stops = resultsOrStops.collect { case Left(stopped) => stopped }
    val results = resultsOrStops.collect { case Right(res) => res }
    
    if (stops.nonEmpty)
      throw WorkflowStoppedToAwaitManyConditions(stops)
    else {
      assert(results.size == elems.size)
      results
    }
  }
}
