package atomicflow.impl.db

import atomicflow.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.slf4j.LoggerFactory

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executor, ExecutorService, Executors, ThreadFactory}
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

  private val RequeueBackoffBase: FiniteDuration = 1.second
  private val RequeueBackoffCap: FiniteDuration = 5.minutes

  private def backoffDelay(attempts: Int): FiniteDuration = {
    val baseNanos = RequeueBackoffBase.toNanos
    val delayNanos = baseNanos * (1L << math.min(attempts, 30))
    (delayNanos.nanos) min RequeueBackoffCap
  }

  private val permitCounts = new ConcurrentHashMap[WorkflowId, java.lang.Integer]()

  private def atCapacity(workflowId: WorkflowId): Boolean =
    permitCounts.getOrDefault(workflowId, java.lang.Integer.valueOf(0)).intValue() >=
      settings.maxConcurrentInstances(workflowId)

  private def acquirePermit(workflowId: WorkflowId): Unit =
    permitCounts.merge(workflowId, java.lang.Integer.valueOf(1), (a: Integer, b: Integer) =>
      java.lang.Integer.valueOf(a + b)
    )

  private def releasePermit(workflowId: WorkflowId): Unit =
    permitCounts.computeIfPresent(
      workflowId,
      (_: WorkflowId, v: java.lang.Integer) => if (v <= 1) null else java.lang.Integer.valueOf(v - 1)
    )

  /** Fault-injection point around a dispatched run: by default identity, a test may
    * replace it to simulate an infrastructure failure that aborts a claimed run
    * before the body runs (producing a transient, terminal-unset, lease-still-ours
    * outcome). Package-private test hook; write from test threads only.
    */
  @volatile
  private[atomicflow] var testRunWrapper: (() => Unit) => Unit = run => run()

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
      instanceKey: WorkflowInstanceKey,
      scope: String,
      createdAt: java.time.Instant,
      scheduledAt: java.time.Instant,
      attempts: Int
  )

  private final case class ClaimedInstance(
      instanceId: WorkflowInstanceId,
      worker: String,
      token: Long,
      attempts: Int,
      createdAt: java.time.Instant
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
          SELECT w.workflow_id, w.workflow_instance_key, w.scope, w.created_at, w.scheduled_at, w.attempts,
                 ROW_NUMBER() OVER (PARTITION BY w.workflow_id ORDER BY w.scheduled_at, w.workflow_instance_key) AS share
          FROM workflow_wakeups w
          JOIN workflow_instances i
            ON i.workflow_id = w.workflow_id AND i.workflow_instance_key = w.workflow_instance_key AND i.scope = w.scope
          WHERE w.scheduled_at <= $now
            AND i.terminal_state IS NULL
            AND w.workflow_id = ANY($ids)
        )
        SELECT r.workflow_id, r.workflow_instance_key, r.scope, r.created_at, r.scheduled_at, r.attempts
        FROM ranked r
        JOIN workflow_wakeups w
          ON w.workflow_id = r.workflow_id AND w.workflow_instance_key = r.workflow_instance_key AND w.scope = r.scope
        WHERE r.share <= ${settings.perWorkflowBatchShare}
        ORDER BY r.scheduled_at, r.workflow_id, r.workflow_instance_key
        LIMIT ${settings.wakeupBatchSize}
        FOR UPDATE OF w SKIP LOCKED
      """
    runtime.runTransaction {
      for {
        rows <- query.query[ClaimRow].to[Vector]
        claimed <- rows.foldLeftM(Vector.empty[ClaimedInstance]) { (acc, row) =>
          if (atCapacity(row.workflowId)) deferRowIO(row, now).as(acc)
          else
            runtime.tryAcquireOnceIO(row.workflowId, row.instanceKey, row.scope, runnerWorker, settings.leaseDuration).flatMap {
              case Some(token) =>
                sql"""DELETE FROM workflow_wakeups
                      WHERE workflow_id = ${row.workflowId} AND workflow_instance_key = ${row.instanceKey} AND scope = ${row.scope}
                        AND scheduled_at = ${row.scheduledAt}""".update.run.map { _ =>
                  acquirePermit(row.workflowId)
                  acc :+ ClaimedInstance(WorkflowInstanceId(row.workflowId, row.instanceKey, row.scope), runnerWorker, token, row.attempts, row.createdAt)
                }
              case None => acc.pure[ConnectionIO]
            }
        }
      } yield claimed
    }
  }

  /** Defer a wakeup in place when its workflow is at capacity: push `scheduled_at`
    * out by `capacityRetryDelay` (no lease taken, attempts untouched) so a hot
    * workflow neither starves the FIFO batch nor pins a worker. Matches the row's
    * seen `scheduled_at` so a concurrently requeued/future row is not clobbered.
    */
  private def deferRowIO(row: ClaimRow, now: java.time.Instant): ConnectionIO[Unit] = {
    val deferred = now.plus(java.time.Duration.ofNanos(settings.capacityRetryDelay.toNanos))
    sql"""UPDATE workflow_wakeups
          SET scheduled_at = $deferred
          WHERE workflow_id = ${row.workflowId} AND workflow_instance_key = ${row.instanceKey} AND scope = ${row.scope}
            AND scheduled_at = ${row.scheduledAt}""".update.run.map(_ => ())
  }

  /** Requeues a claimed wakeup after a transient failure. One transaction: the
    * row was deleted at claim, but a racing delivery may have re-inserted it, so
    * an upsert preserves the original `created_at` carried from the claim (a fresh
    * insert records `createdAt`; a conflict keeps the existing row's `created_at`
    * unchanged), pushes `scheduled_at` out to the capped exponential backoff
    * (never earlier than an existing future time), and bumps `attempts` by one.
    */
  private[atomicflow] def requeueTransient(instanceId: WorkflowInstanceId, attempts: Int, createdAt: java.time.Instant): Unit = {
    val now = runtime.clock.instant()
    val scheduled = now.plus(java.time.Duration.ofNanos(backoffDelay(attempts).toNanos))
    runtime.runTransaction {
      sql"""INSERT INTO workflow_wakeups (workflow_id, workflow_instance_key, scope, created_at, scheduled_at, attempts)
            VALUES (${instanceId.workflowId}, ${instanceId.workflowInstanceKey}, ${instanceId.scope}, $createdAt, $scheduled, ${attempts + 1})
            ON CONFLICT (workflow_id, workflow_instance_key, scope) DO UPDATE
            SET created_at = workflow_wakeups.created_at,
                scheduled_at = GREATEST(workflow_wakeups.scheduled_at, EXCLUDED.scheduled_at),
                attempts = workflow_wakeups.attempts + 1""".update.run
    }
    ()
  }

  private def dispatchRun(c: ClaimedInstance): Unit = {
    val workflow = registry(c.instanceId.workflowId)
    try {
      testRunWrapper(() => {
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
      })
    } catch {
      case _: WorkflowNotFoundException =>
        log.warn(s"Claimed workflow instance ${c.instanceId} was not found; wakeup already consumed")
      case e: Throwable =>
        classifyFailure(c, e)
    } finally {
      runtime.releaseLeaseIfOurs(c.instanceId, c.worker, c.token)
      releasePermit(c.instanceId.workflowId)
    }
  }

  /** Classifies a run that ended by throwing, by the instance's durable state
    * rather than the exception type: terminal (including FAILED, which is logged
    * and never rescheduled), lease lost (new owner responsible), or transient
    * (requeued with capped exponential backoff, unbounded retries).
    */
  private def classifyFailure(c: ClaimedInstance, e: Throwable): Unit =
    runtime.readTerminalState(c.instanceId) match {
      case Some("failed") =>
        log.warn(s"Workflow instance ${c.instanceId} failed and will not be rescheduled", e)
      case Some(_) =>
        log.debug(s"Workflow instance ${c.instanceId} reached a terminal state; adopting its outcome")
      case None =>
        if (runtime.leaseStillOurs(c.instanceId, c.worker, c.token)) {
          log.warn(s"Workflow instance ${c.instanceId} ended with a transient failure; requeueing with backoff", e)
          requeueTransient(c.instanceId, c.attempts, c.createdAt)
        } else
          log.debug(s"Workflow instance ${c.instanceId} lost its lease; the new owner is responsible")
    }

  private def dispatchAndAwait(claimed: Vector[ClaimedInstance]): Unit = {
    if (claimed.isEmpty) return
    log.info(s"Job runner claimed ${claimed.size} wakeups this cycle")
    val latch = new CountDownLatch(claimed.size)
    claimed.foreach { c =>
      try {
        pool.execute(() => {
          try dispatchRun(c)
          catch {
            case e: Throwable => log.warn(s"Uncaught error dispatching ${c.instanceId}", e)
          } finally latch.countDown()
        })
      } catch {
        case e: Throwable =>
          log.warn(s"Executor rejected the dispatch of ${c.instanceId}; requeueing", e)
          runtime.releaseLeaseIfOurs(c.instanceId, c.worker, c.token)
          releasePermit(c.instanceId.workflowId)
          requeueTransient(c.instanceId, c.attempts, c.createdAt)
          latch.countDown()
      }
    }
    latch.await()
  }

  private final case class TimerSweepRow(
      timerId: java.util.UUID,
      workflowId: WorkflowId,
      instanceKey: WorkflowInstanceKey,
      scope: String
  )

  private final case class InstanceRow(
      workflowId: WorkflowId,
      instanceKey: WorkflowInstanceKey,
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
    val rows = runtime.runTransaction {
      (fr"""
        SELECT s.timer_id, s.workflow_id, s.workflow_instance_key, s.scope
        FROM workflow_timer_subscriptions s
        JOIN workflow_instances i
          ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.scope = s.scope
        WHERE s.deadline <= $now
          AND i.terminal_state IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM workflow_events e
            WHERE e.workflow_id = s.workflow_id AND e.workflow_instance_key = s.workflow_instance_key AND e.scope = s.scope
              AND e.event_kind = 'TimerFired' AND e.event_key = s.timer_id::text
          )
        ORDER BY s.deadline
        LIMIT ${settings.timerBatchSize}
        FOR UPDATE OF s SKIP LOCKED
      """).query[TimerSweepRow].to[Vector]
    }
    var fired = 0
    rows.foreach { row =>
      if (runtime.runTransaction(runtime.fireTimerSubscriptionIO(row.timerId, row.workflowId, row.instanceKey, row.scope)))
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
    val rows = runtime.runTransaction {
      sql"""SELECT workflow_id, workflow_instance_key, scope FROM workflow_instances
            WHERE cancel_requested_at IS NOT NULL AND terminal_state IS NULL
              AND cancel_requested_at <= $cutoff
            LIMIT 128""".query[InstanceRow].to[Vector]
    }
    var escalated = 0
    rows.foreach { row =>
      if (runtime.escalateTerminated(WorkflowInstanceId(row.workflowId, row.instanceKey, row.scope)))
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
    val rows = runtime.runTransaction {
      sql"""SELECT workflow_id, workflow_instance_key, scope FROM workflow_instances
            WHERE lease_owner IS NOT NULL AND lease_expires_at <= $now AND terminal_state IS NULL
            LIMIT 128""".query[InstanceRow].to[Vector]
    }
    var recovered = 0
    rows.foreach { row =>
      if (runtime.recoverLease(WorkflowInstanceId(row.workflowId, row.instanceKey, row.scope)))
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
