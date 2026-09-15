package atomicflow.impl.db

import atomicflow.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.slf4j.LoggerFactory

import java.util.concurrent.{CountDownLatch, Executor, ExecutorService, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import cats.syntax.all.*

/** The PostgreSQL job runner: a driver loop that claims due wakeups from the
  * shared tables (registry-filtered, with per-workflow fairness, claim-atomic
  * lease acquisition) and dispatches them onto a bounded worker pool, executing
  * each instance through the same body code path as the public run API.
  *
  * Bound to its [[PostgresWorkflowRuntime]]; runner and runtime are one
  * implementation family.
  */
final class PostgresJobRunner private[atomicflow] (
    runtime: PostgresWorkflowRuntime,
    definitions: Seq[Workflow[?, ?]],
    settings: JobRunnerSettings
) extends JobRunner {

  private val log = LoggerFactory.getLogger(getClass)

  private val registry: Map[WorkflowId, Workflow[?, ?]] = {
    val grouped = definitions.groupBy(_.id)
    grouped.collectFirst { case (id, ws) if ws.size > 1 => id } match {
      case Some(id) =>
        throw new IllegalArgumentException(s"Duplicate workflow id in job runner registry: $id")
      case None =>
        grouped.view.mapValues(_.head).toMap
    }
  }

  private val runnerWorker = runtime.workerIdFor("jobrunner")

  private given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private val stopped = new AtomicBoolean(false)

  private val ownedPool: Option[ExecutorService] =
    if (settings.executor.isDefined) None
    else
      Some(
        Executors.newFixedThreadPool(
          settings.workerThreads,
          new ThreadFactory {
            private val n = new java.util.concurrent.atomic.AtomicInteger(0)
            override def newThread(r: Runnable): Thread = {
              val t = new Thread(r, s"atomicflow-jobrunner-worker-${n.incrementAndGet()}")
              t.setDaemon(true)
              t
            }
          }
        )
      )

  private val pool: Executor = settings.executor.getOrElse(ownedPool.get)

  private[atomicflow] def isActive: Boolean = !stopped.get

  private def jittered(d: FiniteDuration): Long = {
    val base = d.toNanos
    val jitter = ((base * 0.2) * (math.random * 2 - 1)).toLong
    math.max(1L, (base + jitter) / 1000000L)
  }

  private final case class ClaimRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String,
      scheduledAt: java.time.Instant,
      attempts: Int
  )

  private final case class ClaimedInstance(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long
  )

  /** The ranked claim: per-workflow ROW_NUMBER over due, non-terminal wakeups of
    * registered workflows, bounded per workflow by `perWorkflowBatchShare` and
    * overall by `wakeupBatchSize`, locking the selected wakeup rows with
    * `FOR UPDATE SKIP LOCKED`. Runs in the caller's transaction so the lease
    * acquire and the wakeup delete are atomic with it.
    */
  private def claimOnce(): Vector[ClaimedInstance] = {
    val now = runtime.clock.instant()
    val ids: List[WorkflowId] = registry.keys.toList
    val query =
      fr"""
        WITH ranked AS (
          SELECT w.workflow_id, w.key, w.scope, w.scheduled_at, w.attempts,
                 ROW_NUMBER() OVER (PARTITION BY w.workflow_id ORDER BY w.scheduled_at, w.key) AS share
          FROM workflow_wakeups w
          JOIN workflow_instances i
            ON i.workflow_id = w.workflow_id AND i.key = w.key AND i.scope = w.scope
          WHERE w.scheduled_at <= $now
            AND i.terminal_state IS NULL
            AND w.workflow_id = ANY($ids)
        )
        SELECT r.workflow_id, r.key, r.scope, r.scheduled_at, r.attempts
        FROM ranked r
        JOIN workflow_wakeups w
          ON w.workflow_id = r.workflow_id AND w.key = r.key AND w.scope = r.scope
        WHERE r.share <= ${settings.perWorkflowBatchShare}
        ORDER BY r.scheduled_at, r.workflow_id, r.key
        LIMIT ${settings.wakeupBatchSize}
        FOR UPDATE OF w SKIP LOCKED
      """
    runtime.runSync {
      for {
        rows <- query.query[ClaimRow].to[Vector]
        claimed <- rows.traverse { row =>
          runtime.tryAcquireOnceIO(row.workflowId, row.key, row.scope, runnerWorker, settings.leaseDuration).flatMap {
            case Some(token) =>
              sql"""DELETE FROM workflow_wakeups
                    WHERE workflow_id = ${row.workflowId} AND key = ${row.key} AND scope = ${row.scope}
                      AND scheduled_at = ${row.scheduledAt}""".update.run
                .map(_ => Some(ClaimedInstance(WorkflowInstanceId(row.workflowId, row.key, row.scope), runnerWorker, token)))
            case None => Option.empty[ClaimedInstance].pure[ConnectionIO]
          }
        }
      } yield claimed.flatten
    }
  }

  private def dispatchRun(c: ClaimedInstance): Unit = {
    val workflow = registry(c.instanceId.workflowId)
    try {
      val result = runtime.runClaimedInstance(
        workflow,
        c.instanceId,
        c.worker,
        c.token,
        settings.leaseDuration,
        settings.leaseAcquireTimeout
      )
      result match {
        case WorkflowRunResult.WorkflowSuspended =>
          log.debug(s"Workflow instance ${c.instanceId} suspended (runner)")
        case other =>
          log.debug(s"Workflow instance ${c.instanceId} finished (runner): $other")
      }
    } catch {
      case _: WorkflowNotFoundException =>
        log.warn(s"Claimed workflow instance ${c.instanceId} was not found; wakeup already consumed")
      case e: Throwable =>
        classifyFailure(c, e)
    }
  }

  /** Classifies a run that ended by throwing, by the instance's durable state
    * rather than the exception type: terminal (including FAILED, which is logged
    * and never rescheduled), lease lost (new owner responsible), or transient
    * (Task 5.3 requeues; for now logged with the wakeup left absent).
    */
  private def classifyFailure(c: ClaimedInstance, e: Throwable): Unit =
    runtime.readTerminalState(c.instanceId) match {
      case Some("failed") =>
        log.warn(s"Workflow instance ${c.instanceId} failed and will not be rescheduled", e)
      case Some(_) =>
        log.debug(s"Workflow instance ${c.instanceId} reached a terminal state; adopting its outcome")
      case None =>
        if (runtime.leaseStillOurs(c.instanceId, c.worker, c.token))
          log.warn(s"Workflow instance ${c.instanceId} ended with a transient failure; not requeued (Task 5.3)", e)
        else
          log.debug(s"Workflow instance ${c.instanceId} lost its lease; the new owner is responsible")
    }

  private def dispatchAndAwait(claimed: Vector[ClaimedInstance]): Unit = {
    if (claimed.isEmpty) return
    log.info(s"Job runner claimed ${claimed.size} wakeups this cycle")
    val latch = new CountDownLatch(claimed.size)
    claimed.foreach { c =>
      pool.execute(() => {
        try dispatchRun(c)
        catch {
          case e: Throwable => log.warn(s"Uncaught error dispatching ${c.instanceId}", e)
        } finally latch.countDown()
      })
    }
    latch.await()
  }

  private def runSweepStubs(): Unit = ()

  private[atomicflow] def runDriverCycle(): Unit = {
    if (stopped.get) throw new IllegalStateException("Job runner is stopped")
    runSweepStubs()
    val claimed = claimOnce()
    lastClaimedBatch = claimed.map(_.instanceId)
    dispatchAndAwait(claimed)
  }

  private[atomicflow] var lastClaimedBatch: Vector[WorkflowInstanceId] = Vector.empty

  private val loopThread = new Thread(
    () => driverLoop(),
    "atomicflow-jobrunner-driver"
  )

  private def driverLoop(): Unit = {
    var backoff = settings.pollInterval
    while (!stopped.get) {
      try {
        runSweepStubs()
        val claimed = claimOnce()
        if (claimed.isEmpty) {
          Thread.sleep(jittered(settings.pollInterval))
          backoff = settings.pollInterval
        } else {
          lastClaimedBatch = claimed.map(_.instanceId)
          dispatchAndAwait(claimed)
          backoff = settings.pollInterval
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          return
        case e: Throwable =>
          log.warn("Job runner cycle failed", e)
          Thread.sleep(backoff.toMillis)
          backoff = (backoff * 2) min (settings.pollInterval * 10)
      }
    }
  }

  loopThread.setDaemon(true)
  loopThread.start()

  override def stop(gracePeriod: FiniteDuration): Unit = {
    if (!stopped.getAndSet(true)) {
      runtime.clearActiveRunner(this)
      loopThread.interrupt()
      try loopThread.join(gracePeriod.toMillis)
      catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
      ownedPool match {
        case Some(es) =>
          es.shutdown()
          try {
            if (!es.awaitTermination(gracePeriod.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)) {
              es.shutdownNow()
            }
          } catch {
            case _: InterruptedException => Thread.currentThread().interrupt()
          }
        case None => ()
      }
    }
  }
}
