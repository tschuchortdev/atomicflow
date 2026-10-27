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

/** Claims workflow wakeups and executes them on a bounded worker pool. A claim
  * acquires the execution lease and removes the wakeup in one transaction; the
  * claim is FIFO by wakeup age. Only registered workflow definitions are run.
  */
final class PostgresJobRunner private[atomicflow] (
    runtime: PostgresWorkflowRuntime,
    definitions: Seq[Workflow[?, ?]],
    settings: JobRunnerSettings,
    startLoop: Boolean = true
) extends JobRunner {

  private val logger = LoggerFactory.getLogger(getClass)

  private val workflowDefinitionsRegistry: Map[WorkflowId, Workflow[?, ?]] = {
    val grouped = definitions.groupBy(_.id)
    grouped.collectFirst { case (id, ws) if ws.size > 1 => id } match {
      case Some(id) =>
        throw new IllegalArgumentException(s"Duplicate workflow id in job runner registry: $id")
      case None =>
        grouped.view.mapValues(_.head).toMap
    }
  }

  private val isStopped = new AtomicBoolean(false)

  /** Tests can replace this function to fail a claimed run before its body starts.
    * The lease remains owned and the instance has no terminal state, allowing
    * tests to verify that the runner schedules another attempt.
    */
  @volatile
  private[atomicflow] var testRunWrapper: (() => Unit) => Unit = run => run()

  private val ownedWorkerPool: Option[ExecutorService] =
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

  private val workerExecutor: Executor = settings.executor.getOrElse(ownedWorkerPool.get)

  private def jitteredPollDelayMillis(interval: FiniteDuration): Long = {
    val base = interval.toNanos
    val jitter = ((base * 0.2) * (math.random * 2 - 1)).toLong
    math.max(1L, (base + jitter) / 1000000L)
  }

  private final case class DueWakeupRow(
      workflowId: WorkflowId,
      instanceKey: WorkflowInstanceKey,
      scope: ScopePath,
      createdAt: java.time.Instant,
      attempts: Int
  )

  private final case class ClaimedInstance(
      instanceId: WorkflowInstanceId,
      token: Long,
      attempts: Int
  )

  /** The claim: FIFO over non-terminal wakeups of registered workflows, oldest
    * `created_at` first, bounded by `wakeupBatchSize`, locking the selected
    * wakeup rows with `FOR UPDATE SKIP LOCKED`. Runs in the caller's transaction
    * so the lease acquire and the wakeup delete are atomic with it. There is no
    * due-time filter and no per-workflow ranking: every wakeup row means "this
    * instance needs execution", and a technically failing instance re-enters the
    * FIFO at the back (its restore records a fresh `created_at`), which is what
    * throttles it without any per-instance retry state. Instances holding a
    * live lease are excluded from the candidate selection (an event-driven
    * wakeup upserted while its instance is running would otherwise sit at the
    * FIFO head and starve newer work behind it); the conditional lease acquire
    * below remains the final correctness check against races.
    */
  private def claimDueWakeups(): Vector[ClaimedInstance] = {
    val now = runtime.clock.instant()
    val ids: List[WorkflowId] = workflowDefinitionsRegistry.keys.toList
    val query =
      fr"""
        SELECT w.workflow_id, w.workflow_instance_key, w.workflow_instance_scope, w.created_at, w.attempts
        FROM workflow_wakeups w
        JOIN workflow_instances i
          ON i.workflow_id = w.workflow_id AND i.workflow_instance_key = w.workflow_instance_key AND i.workflow_instance_scope = w.workflow_instance_scope
        WHERE i.terminal_state IS NULL
          AND (i.lease_expires_at IS NULL OR i.lease_expires_at <= $now)
          AND w.workflow_id = ANY($ids)
        ORDER BY w.created_at, w.workflow_instance_key
        LIMIT ${settings.wakeupBatchSize}
        FOR UPDATE OF w SKIP LOCKED
      """
    runtime.runTransaction {
      for {
        rows <- query.query[DueWakeupRow].to[Vector]
        claimed <- rows.foldLeftM(Vector.empty[ClaimedInstance]) { (acc, row) =>
          runtime.tryAcquireLeaseQ(row.workflowId, row.instanceKey, row.scope, settings.leaseDuration).flatMap {
            case Some(token) =>
              sql"""
                    DELETE FROM workflow_wakeups
                    WHERE workflow_id = ${row.workflowId} AND workflow_instance_key = ${row.instanceKey} AND workflow_instance_scope = ${row.scope}
              """
                .update.run
                .map { _ =>
                  acc :+ ClaimedInstance(WorkflowInstanceId(row.workflowId, row.instanceKey, row.scope), token, row.attempts)
                }
            case None => acc.pure[ConnectionIO]
          }
        }
      } yield claimed
    }
  }

  private def dispatchRun(c: ClaimedInstance): Unit = {
    val workflow = workflowDefinitionsRegistry(c.instanceId.workflowId)
    try {
      testRunWrapper(() => {
        val result = runtime.runClaimedInstance(
          workflow,
          c.instanceId,
          c.token
        )(using settings.throwableCacheable)
        result match {
          case WorkflowRunResult.Failed(cause) =>
            // The workflow reached its durable terminal FAILED outcome (the
            // cause is the decoded user failure): never re-execute.
            logger.warn(s"Workflow instance ${c.instanceId} failed and will not be rescheduled", cause)
          case WorkflowRunResult.WorkflowSuspended =>
            logger.debug(s"Workflow instance ${c.instanceId} suspended (runner)")
          case other =>
            logger.debug(s"Workflow instance ${c.instanceId} finished (runner): $other")
        }
      })
      // Normal outcome (result, suspension, terminal decode — FAILED included):
      // the lease is ours and the instance is either suspended (subscriptions
      // cover it) or terminal, so release and move on.
      runtime.releaseLeaseIfStillOwned(c.instanceId, c.token)
    } catch {
      case _: WorkflowNotFoundException =>
        // The instance was deleted between claim and run (e.g. discard-existing):
        // its lease row is gone with it, nothing to release or restore.
        logger.warn(s"Claimed workflow instance ${c.instanceId} was not found; wakeup already consumed")

      case _: LeaseLostException =>
        // Another owner took over (or the instance was terminated): that owner
        // is responsible for the instance and its scheduling.
        logger.debug(s"Workflow instance ${c.instanceId} lost its lease; the new owner is responsible")

      case e: WorkflowTechnicalException =>
        logger.warn(s"Workflow instance ${c.instanceId} ended with a technical failure; restoring its wakeup", e)
        restoreAfterFailure(c)

      case e: Throwable =>
        // An unknown exception escaping the run is a library bug, not a workflow
        // failure. It must never be persisted as one; restoring the wakeup keeps
        // the instance re-executable (and, if deterministic, visibly retried).
        logger.error(s"Workflow instance ${c.instanceId} ended with an unexpected internal error; treating it as technical", e)
        restoreAfterFailure(c)
    }
  }

  /** Restores the wakeup and releases the lease after a technical failure. If
    * the restore transaction itself cannot commit (the database may still be
    * down), the lease is deliberately left in place: the lease-expiry recovery
    * sweep then clears it and upserts the wakeup, so the instance is never left
    * with neither a wakeup nor an owned lease. A lease that cannot be released
    * on a terminal outcome likewise expires into the recovery sweep.
    */
  private def restoreAfterFailure(c: ClaimedInstance): Unit =
    try runtime.restoreWakeupAndReleaseLease(c.instanceId, c.token, c.attempts)
    catch {
      case e: Throwable =>
        logger.warn(
          s"Could not restore the wakeup of ${c.instanceId}; leaving its lease for the recovery sweep",
          e
        )
    }

  private def dispatchAndAwait(claimed: Vector[ClaimedInstance]): Unit = {
    if (claimed.isEmpty) return
    logger.info(s"Job runner claimed ${claimed.size} wakeups this cycle")
    val latch = new CountDownLatch(claimed.size)
    claimed.foreach { c =>
      try {
        workerExecutor.execute(() => {
          try dispatchRun(c)
          catch {
            case e: Throwable => logger.warn(s"Uncaught error dispatching ${c.instanceId}", e)
          } finally latch.countDown()
        })
      } catch {
        case e: Throwable =>
          logger.warn(s"Executor rejected the dispatch of ${c.instanceId}; restoring its wakeup", e)
          restoreAfterFailure(c)
          latch.countDown()
      }
    }
    latch.await()
  }

  private final case class TimerSweepRow(
      timerId: java.util.UUID,
      workflowId: WorkflowId,
      instanceKey: WorkflowInstanceKey,
      scope: ScopePath
  )

  private final case class InstanceRow(
      workflowId: WorkflowId,
      instanceKey: WorkflowInstanceKey,
      scope: ScopePath
  )

  /** Background timer firing. Claims due, not-yet-fired timer
    * subscriptions of non-terminal instances in deadline order, bounded by
    * `timerBatchSize`, locking the subscription rows with `FOR UPDATE OF s SKIP
    * LOCKED` (the claim; no delete). Each claimed row is then fired in its own
    * transaction via the shared [[PostgresWorkflowRuntime.fireTimerSubscriptionQ]]
    * primitive, which re-checks no event exists under the lock. The subscription
    * row survives firing. Definition-agnostic: services every workflow in the
    * shared tables. Returns the number of timers fired.
    */
  private[atomicflow] def runTimerSweep(): Int = {
    val now = runtime.clock.instant()
    val rows = runtime.runTransaction {
      fr"""
        SELECT s.timer_id, s.workflow_id, s.workflow_instance_key, s.workflow_instance_scope
        FROM workflow_timer_subscriptions s
        JOIN workflow_instances i
          ON i.workflow_id = s.workflow_id AND i.workflow_instance_key = s.workflow_instance_key AND i.workflow_instance_scope = s.workflow_instance_scope
        WHERE s.deadline <= $now
          AND i.terminal_state IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM workflow_events e
            WHERE e.workflow_id = s.workflow_id AND e.workflow_instance_key = s.workflow_instance_key AND e.workflow_instance_scope = s.workflow_instance_scope
              AND e.event_kind = 'TimerFired' AND e.event_key = s.timer_id::text
          )
        ORDER BY s.deadline
        LIMIT ${settings.timerBatchSize}
        FOR UPDATE OF s SKIP LOCKED
      """
        .query[TimerSweepRow].to[Vector]
    }
    var fired = 0
    rows.foreach { row =>
      if (runtime.runTransaction(runtime.fireTimerSubscriptionQ(row.timerId, row.workflowId, row.instanceKey, row.scope)))
        fired += 1
    }
    if (fired > 0) logger.debug(s"Timer sweep fired $fired due timers")
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
      sql"""
            SELECT workflow_id, workflow_instance_key, workflow_instance_scope FROM workflow_instances
            WHERE cancel_requested_at IS NOT NULL AND terminal_state IS NULL
              AND cancel_requested_at <= $cutoff
            LIMIT 128
      """
        .query[InstanceRow].to[Vector]
    }
    var escalated = 0
    rows.foreach { row =>
      val id = WorkflowInstanceId(row.workflowId, row.instanceKey, row.scope)
      if (runtime.escalateTerminated(id)) escalated += 1
    }
    if (escalated > 0) logger.info(s"Escalation sweep escalated $escalated cancellations to TERMINATED")
    escalated
  }

  /** Lease recovery: revokes every instance whose lease has expired — clearing
    * the expiry and advancing the fencing token, so the crashed run is fenced out
    * by the next guarded write — and upserts the non-terminal instance's wakeup
    * so it is replayed from its Step cache. Terminal instances only get their
    * stale lease revoked (a failed post-completion release must not linger);
    * the expiry is re-checked inside the recovery transaction itself, so a lease
    * renewed or taken over after selection is left alone. Bounded batch of 128.
    * Idempotent. Returns the number of leases recovered.
    */
  private[atomicflow] def runRecoverySweep(): Int = {
    val now = runtime.clock.instant()
    val rows = runtime.runTransaction {
      sql"""
            SELECT workflow_id, workflow_instance_key, workflow_instance_scope FROM workflow_instances
            WHERE lease_expires_at IS NOT NULL AND lease_expires_at <= $now
            LIMIT 128
      """
        .query[InstanceRow].to[Vector]
    }
    var recovered = 0
    rows.foreach { row =>
      if (runtime.recoverLease(WorkflowInstanceId(row.workflowId, row.instanceKey, row.scope)))
        recovered += 1
    }
    if (recovered > 0) logger.info(s"Lease recovery sweep recovered $recovered expired leases")
    recovered
  }

  /** Child-close delivery: applies pending `ParentClosePolicy` closures
    * recorded as rows in the dedicated `child_close_requests` table, and
    * detaches children whose parent rows are gone. Each child is closed in its
    * own short transaction so no transaction ever locks another instance's
    * rows. Idempotent. Returns the number of children closed.
    */
  private[atomicflow] def runChildCloseSweep(): Int =
    runtime.closePendingChildClosures(128)

  private var lastTimerSweep: Long = 0L
  private var lastEscalationSweep: Long = 0L
  private var lastRecoverySweep: Long = 0L
  private var lastChildCloseSweep: Long = 0L
  private var lastFullScan: Long = 0L

  /** Two asynchronous wakeup jobs back the liveness guarantee:
    *
    * Writers (signal sends and terminal transitions) only append events, so
    * their transactions stay short and cannot deadlock against subscribers. A
    * best-effort watermark dispatcher (`dispatchNewEvents`) replays events after
    * a stored watermark and upserts the wakeups they imply, giving low-latency
    * wakeups for the common case where the subscription already exists when the
    * event is appended. A periodic full subscription scan
    * (`scanAllSubscriptions`) re-examines every pending signal/completion
    * subscription and wakes every non-terminal instance with a visible,
    * unconsumed candidate — the correctness guarantee for the rare race where a
    * subscription and an event commit concurrently, the dispatcher misses the
    * subscription and advances its watermark past the event, so the next scan
    * must re-check all subscriptions. The full scan cadence is
    * `fullSubscriptionScanInterval` (default 1 minute).
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
    if (now - lastChildCloseSweep >= settings.sweepInterval.toNanos) {
      lastChildCloseSweep = now
      runChildCloseSweep()
    }
    runtime.dispatchNewEvents(settings.eventDispatchBatchSize)
    if (now - lastFullScan >= settings.fullSubscriptionScanInterval.toNanos) {
      lastFullScan = now
      runtime.scanAllSubscriptions()
    }
  }

  private[atomicflow] def runDriverCycle(): Unit = {
    if (isStopped.get)
      throw new IllegalStateException("Job runner is stopped")
    runSweepsDue()
    val claimed = claimDueWakeups()
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
    while (!isStopped.get) {
      try {
        runSweepsDue()
        val claimed = claimDueWakeups()
        if (claimed.isEmpty) {
          Thread.sleep(jitteredPollDelayMillis(settings.pollInterval))
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
          logger.warn("Job runner cycle failed", e)
          Thread.sleep(backoff.toMillis)
          backoff = (backoff * 2) min (settings.pollInterval * 10)
      }
    }
  }

  loopThread.setDaemon(true)
  if (startLoop) loopThread.start()

  override def stop(gracePeriod: FiniteDuration): Unit = {
    if (!isStopped.getAndSet(true)) {
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
      ownedWorkerPool match {
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
