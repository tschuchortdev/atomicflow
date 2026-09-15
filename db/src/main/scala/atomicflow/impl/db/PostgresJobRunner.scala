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
    settings: JobRunnerSettings,
    startLoop: Boolean = true
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
      )(using settings.throwableCacheable)
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

  private final case class TimerSweepRow(
      subscriptionId: java.util.UUID,
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  )

  private final case class InstanceRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey,
      scope: String
  )

  /** Path 1 timer firing: the scheduler sweep. Claims due, not-yet-fired timer
    * subscriptions of non-terminal instances in deadline order, bounded by
    * `timerBatchSize`, locking the subscription rows with `FOR UPDATE OF s SKIP
    * LOCKED` (the claim; no delete). Each claimed row is then fired in its own
    * transaction via the shared [[PostgresWorkflowRuntime.fireTimerSubscriptionIO]]
    * primitive, which re-checks no event exists under the lock. The subscription
    * row survives firing. Definition-agnostic: services every workflow in the
    * shared tables. Returns the number of timers fired.
    */
  private[atomicflow] def runTimerSweep(): Int = {
    val now = runtime.clock.instant()
    val rows = runtime.runSync {
      (fr"""
        SELECT s.subscription_id, s.workflow_id, s.key, s.scope
        FROM workflow_timer_subscriptions s
        JOIN workflow_instances i
          ON i.workflow_id = s.workflow_id AND i.key = s.key AND i.scope = s.scope
        WHERE s.deadline <= $now
          AND i.terminal_state IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM workflow_events e
            WHERE e.workflow_id = s.workflow_id AND e.key = s.key AND e.scope = s.scope
              AND e.event_kind = 'TimerFired' AND e.event_key = s.subscription_id::text
          )
        ORDER BY s.deadline
        LIMIT ${settings.timerBatchSize}
        FOR UPDATE OF s SKIP LOCKED
      """).query[TimerSweepRow].to[Vector]
    }
    var fired = 0
    rows.foreach { row =>
      if (runtime.runSync(runtime.fireTimerSubscriptionIO(row.subscriptionId, row.workflowId, row.key, row.scope)))
        fired += 1
    }
    if (fired > 0) log.debug(s"Timer sweep fired $fired due timers")
    fired
  }

  /** Cancellation escalation: turns instances whose `cancel_requested_at` is past
    * `cancelTimeout` and that are not yet terminal into `TERMINATED` via the same
    * guarded transition as `runtime.terminate`. Bounded batch of 128. Idempotent.
    * Returns the number of instances escalated.
    */
  private[atomicflow] def runEscalationSweep(): Int = {
    val now = runtime.clock.instant()
    val cutoff = now.minus(java.time.Duration.ofNanos(settings.cancelTimeout.toNanos))
    val rows = runtime.runSync {
      sql"""SELECT workflow_id, key, scope FROM workflow_instances
            WHERE cancel_requested_at IS NOT NULL AND terminal_state IS NULL
              AND cancel_requested_at <= $cutoff
            LIMIT 128""".query[InstanceRow].to[Vector]
    }
    var escalated = 0
    rows.foreach { row =>
      if (runtime.escalateTerminated(WorkflowInstanceId(row.workflowId, row.key, row.scope)))
        escalated += 1
    }
    if (escalated > 0) log.info(s"Escalation sweep escalated $escalated cancellations to TERMINATED")
    escalated
  }

  /** Lease recovery: clears the owner of every non-terminal instance whose lease
    * has expired (the crashed run is fenced out by the next acquire's
    * fencing-token bump) and upserts the instance's wakeup so it is replayed
    * from its Step cache. Bounded batch of 128. Idempotent. Returns the number
    * of leases recovered.
    */
  private[atomicflow] def runRecoverySweep(): Int = {
    val now = runtime.clock.instant()
    val rows = runtime.runSync {
      sql"""SELECT workflow_id, key, scope FROM workflow_instances
            WHERE lease_owner IS NOT NULL AND lease_expires_at <= $now AND terminal_state IS NULL
            LIMIT 128""".query[InstanceRow].to[Vector]
    }
    var recovered = 0
    rows.foreach { row =>
      if (runtime.recoverLease(WorkflowInstanceId(row.workflowId, row.key, row.scope)))
        recovered += 1
    }
    if (recovered > 0) log.info(s"Lease recovery sweep recovered $recovered expired leases")
    recovered
  }

  private var lastTimerSweep: Long = 0L
  private var lastEscalationSweep: Long = 0L
  private var lastRecoverySweep: Long = 0L

  /** Runs each sweep when its cadence has elapsed: the timer sweep on
    * `timerSweepInterval`, escalation and recovery on `sweepInterval`. First
    * cycle runs all three.
    */
  private def runSweepsDue(): Unit = {
    val now = System.nanoTime()
    if (now - lastTimerSweep >= settings.timerSweepInterval.toNanos) {
      lastTimerSweep = now
      runTimerSweep()
    }
    if (now - lastEscalationSweep >= settings.sweepInterval.toNanos) {
      lastEscalationSweep = now
      runEscalationSweep()
    }
    if (now - lastRecoverySweep >= settings.sweepInterval.toNanos) {
      lastRecoverySweep = now
      runRecoverySweep()
    }
  }

  private[atomicflow] def runDriverCycle(): Unit = {
    if (stopped.get) throw new IllegalStateException("Job runner is stopped")
    runSweepsDue()
    val claimed = claimOnce()
    lastClaimedBatch = claimed.map(_.instanceId)
    dispatchAndAwait(claimed)
  }

  /** The most recent batch of workflow instance ids claimed, either by a
    * `runDriverCycle` call or by the running driver loop. Package-private test
    * hook; written by a driver thread and read by test threads, so it is
    * `@volatile`. Caveat: a concurrently running driver loop can clobber the
    * value a `runDriverCycle` caller just set.
    */
  @volatile
  private[atomicflow] var lastClaimedBatch: Vector[WorkflowInstanceId] = Vector.empty

  private val loopThread = new Thread(
    () => driverLoop(),
    "atomicflow-jobrunner-driver"
  )

  private def driverLoop(): Unit = {
    var backoff = settings.pollInterval
    while (!stopped.get) {
      try {
        runSweepsDue()
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
  if (startLoop) loopThread.start()

  override def stop(gracePeriod: FiniteDuration): Unit = {
    if (!stopped.getAndSet(true)) {
      runtime.clearActiveRunner(this)
      val deadline = System.nanoTime() + gracePeriod.toNanos
      loopThread.interrupt()
      try {
        val remaining = deadline - System.nanoTime()
        if (remaining > 0) {
          loopThread.join(remaining / 1000000L, (remaining % 1000000L).toInt)
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
      ownedPool match {
        case Some(es) =>
          es.shutdown()
          try {
            val remaining = deadline - System.nanoTime()
            if (remaining > 0) {
              if (!es.awaitTermination(remaining, java.util.concurrent.TimeUnit.NANOSECONDS)) {
                es.shutdownNow()
              }
            } else {
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
