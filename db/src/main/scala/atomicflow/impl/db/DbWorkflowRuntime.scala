package atomicflow.impl.db

import atomicflow.*
import atomicflow.Constants.libraryVersion
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.WorkflowRuntime.{StoppedWorkflow, WorkflowStoppedToAwaitManyConditions, WorkflowStoppedToAwaitSignal, WorkflowStoppedToAwaitTimer, WorkflowStoppedToAwaitWorkflow, WorkflowStoppedToWait}
import atomicflow.impl.Sha256Fingerprinter
import atomicflow.impl.db.DbWorkflowRuntime.given
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints}
import cats.Monad
import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.circe.jsonb.implicits.*
import doobie.postgres.implicits.*
import org.flywaydb.core.Flyway
import org.postgresql.util.PSQLException
import ox.{discard, raceResult}

import java.time.{Clock, Instant}
import java.util.UUID
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, TimeoutException}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try
import scala.collection.immutable

// TODO: rename to PostgresWorkflowRuntime
class DbWorkflowRuntime protected (ds: DataSource)(using awaitConnectionEc: ExecutionContext, clk: Clock = Clock.systemUTC())
  extends WorkflowRuntime with WorkflowRuntime.DefaultGenerateIdsMixin {

  protected val xa = Transactor.fromDataSource[IO](ds, awaitConnectionEc)

  private def runSync[A](fa: ConnectionIO[A]): A = {
    fa.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)
  }

  override def createWorkflowInstance[In, Out](
                                                workflow: Workflow[In, Out],
                                                instanceId: WorkflowInstanceKey,
                                                in: In
                                              )(
                                                using Cacheable[In]
                                              ): Unit = {
    given WorkflowInstanceMeta = WorkflowInstanceMeta(instanceId, workflow.meta)

    val workflowId = workflow.meta.workflowId
    val inputBytes = Cacheable[In].serialize(in).asInstanceOf[Array[Byte]]

    runSync {
      sql"""
        INSERT INTO workflow_instance (workflow_id, key, input)
        VALUES ($workflowId, $instanceId, $inputBytes)
        ON CONFLICT (workflow_id, key) DO UPDATE
        SET input = workflow_instance.input
        WHERE workflow_instance.input = EXCLUDED.input
      """.update.run
    } match {
      case 0 => throw new WorkflowInputConflictException()
      case _ => ()
    }
  }

  override def createWorkflowInstanceDiscardExisting[In, Out](
                                                               workflow: Workflow[In, Out],
                                                               instanceId: WorkflowInstanceKey,
                                                               in: In
                                                             )(
                                                               using Cacheable[In]
                                                             ): Boolean = {
    given WorkflowInstanceMeta = WorkflowInstanceMeta(instanceId, workflow.meta)

    val workflowId = workflow.meta.workflowId
    val inputBytes = Cacheable[In].serialize(in).asInstanceOf[Array[Byte]]

    runSync {
      for {
        deleted <- sql"DELETE FROM workflow_instance WHERE workflow_id = $workflowId AND key = $instanceId".update.run
        _ <- sql"""
          INSERT INTO workflow_instance (workflow_id, key, input)
          VALUES ($workflowId, $instanceId, $inputBytes)
        """.update.run
      } yield deleted > 0
    }
  }

  override def createAndRunWorkflowInstance[In, Out](
                                                    workflow: Workflow[In, Out],
                                                    instanceId: WorkflowInstanceKey,
                                                    in: In
                                                  )(
                                                    using Cacheable[In], Cacheable[Out], WorkflowRunSettings
                                                  ): Either[StoppedWorkflow[Out], Out] = {
    createWorkflowInstance(workflow, instanceId, in)
    runWorkflowInstance(workflow, instanceId)
  }

  // TODO: What if workflow doesn't complete within 5 minutes? -> Bug: concurrent executions possible
  private val lockDuration = java.time.Duration.ofMinutes(5)

  override def runWorkflowInstance[In, Out](
                                             workflow: Workflow[In, Out],
                                             instanceId: WorkflowInstanceKey
                                           )(
                                             using Cacheable[In], Cacheable[Out], WorkflowRunSettings
                                           ): Either[StoppedWorkflow[Out], Out] = {
    given WorkflowInstanceMeta = WorkflowInstanceMeta(instanceId, workflow.meta)

    val workflowId = workflow.meta.workflowId
    val now = Instant.now(clk)
    val lockUntil = now.plus(lockDuration)

    val serializedInput: Array[Byte] = runSync {
      sql"SELECT key, locked_until FROM workflow_instance WHERE workflow_id = $workflowId AND key = $instanceId FOR UPDATE"
        .query[(WorkflowInstanceKey, Option[Instant])]
        .option
        .flatMap {
          case None =>
            throw new WorkflowNotFoundException(workflowId, Some(instanceId))

          case Some((_, Some(lockedUntil))) if now.isBefore(lockedUntil) =>
            throw new WorkflowLockedException()

          case Some((_, _)) =>
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE workflow_id = $workflowId AND key = $instanceId RETURNING input"
              .update
              .withUniqueGeneratedKeys[Array[Byte]]("input")
        }
    }

    val settings = summon[WorkflowRunSettings]
    val ctx = WorkflowContext(
      workflowInstanceMeta = WorkflowInstanceMeta(instanceId, workflow.meta),
      workflowRuntime = this,
      defaultCacheTtl = settings.defaultCacheTtl,
      stepIdempotencyIdOverrides = settings.stepIdempotencyIdOverrides
    )

    try {
      val result = raceResult(
        workflow.body(ctx, Cacheable[In].deserialize(IArray.unsafeFromArray(serializedInput))),
        ox.forever {
          Thread.sleep(lockDuration.minus(java.time.Duration.ofMinutes(1)).toMillis)
          val nextLockUntil = Instant.now(clk).plus(lockDuration)
          try {
            ox.timeout(30.seconds) {
              runSync {
                sql"UPDATE workflow_instance SET locked_until = $nextLockUntil WHERE workflow_id = $workflowId AND key = $instanceId"
                  .update
                  .run
                  .void
              }
            }
          } catch {
            case e: TimeoutException =>
              throw new RuntimeException("Could not renew lock on workflow instance in time", e)
          }
        }
      )

      val resultBytes = Cacheable[Out].serialize(result).asInstanceOf[Array[Byte]]
      runSync {
        sql"""
          INSERT INTO workflow_result (workflow_id, workflow_instance_key, result)
          VALUES ($workflowId, $instanceId, $resultBytes)
          ON CONFLICT (workflow_id, workflow_instance_key) DO NOTHING
        """.update.run
      }

      Right(result)
    } catch {
      case stopped: WorkflowStoppedToWait =>
        scheduleWakeups(workflowId, instanceId, stopped)
        val stoppedWorkflowKey = (workflowId, instanceId)
        Left(new StoppedWorkflow[Out](workflowId, instanceId) {
          override def addContinueListener(onWorkflowContinued: Try[Either[StoppedWorkflow[Out], Out]] => Unit): StoppedWorkflow.ListenerHandle = {
            workflowCallbacks.updateWith(stoppedWorkflowKey) { valueMaybe =>
              Some(valueMaybe.getOrElse(immutable.HashSet.empty)
                .incl(onWorkflowContinued.asInstanceOf[Try[Either[StoppedWorkflow[Any], Any]] => Unit]))
            }

            new StoppedWorkflow.ListenerHandle {
              override def remove(): Unit = workflowCallbacks.updateWith(stoppedWorkflowKey) { valueMaybe =>
                val updated = valueMaybe.getOrElse(throw new AssertionError(
                  s"Expected at least one callback to be defined for workflow instance $stoppedWorkflowKey"
                ))
                  .excl(onWorkflowContinued.asInstanceOf[Try[Either[StoppedWorkflow[Any], Any]] => Unit])
                if updated.nonEmpty then Some(updated) else None
              }.discard
            }
          }
        })
    } finally {
      runSync {
        sql"UPDATE workflow_instance SET locked_until = NULL WHERE workflow_id = $workflowId AND key = $instanceId"
          .update
          .run
          .void
      }
    }
  }

  private def scheduleWakeups(workflowId: WorkflowId, instanceKey: WorkflowInstanceKey, stopped: WorkflowStoppedToWait): Unit = {
    stopped match {
      case WorkflowStoppedToAwaitTimer(expectedRestartTime) =>
        scheduleWakeupOnTimer(workflowId, instanceKey, expectedRestartTime)
      case WorkflowStoppedToAwaitSignal(signal) =>
        scheduleWakeupOnSignal(workflowId, instanceKey, signal)
      case WorkflowStoppedToAwaitWorkflow(awaitedWorkflowId, awaitedInstanceKey) =>
        scheduleWakeupOnWorkflowCompletion(workflowId, instanceKey, awaitedWorkflowId, awaitedInstanceKey)
      case WorkflowStoppedToAwaitManyConditions(stops) =>
        stops.foreach(stop => scheduleWakeups(workflowId, instanceKey, stop))
    }
  }

  private val workflowCallbacks = new collection.concurrent.TrieMap[(WorkflowId, WorkflowInstanceKey), immutable.HashSet[Try[Either[StoppedWorkflow[Any], Any]] => Unit]]()

  override def getWorkflowInstancesByPrefix(workflowId: WorkflowId, keyPrefix: WorkflowInstanceKey): Vector[WorkflowInstanceKey] = {
    runSync {
      sql"SELECT key FROM workflow_instance WHERE workflow_id = $workflowId AND key LIKE $keyPrefix || '%'"
        .query[WorkflowInstanceKey]
        .to[Vector]
    }
  }

  override def getUnfinishedWorkflowInstances(workflowId: WorkflowId, includeWaiting: Boolean = false, limit: Int = -1): Vector[WorkflowInstanceKey] = {
    val baseSql = fr"""
      SELECT key FROM workflow_instance
      WHERE workflow_id = $workflowId
        AND key NOT IN (SELECT workflow_instance_key FROM workflow_result WHERE workflow_id = $workflowId)
    """
    val limitedSql = if (limit > 0) baseSql ++ fr"LIMIT $limit" else baseSql
    runSync(limitedSql.query[WorkflowInstanceKey].to[Vector])
  }

  override def deleteWorkflowInstancesByPrefix(workflowId: WorkflowId, instanceKeyPrefix: WorkflowInstanceKey): Long = {
    runSync {
      sql"""
        DELETE FROM workflow_instance
        WHERE workflow_id = $workflowId AND key LIKE $instanceKeyPrefix || '%'
      """.update.run.map(_.toLong)
    }
  }

  override def isWorkflowInstanceCompleted(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey): Boolean = {
    runSync {
      sql"""
        SELECT EXISTS(SELECT 1 FROM workflow_instance WHERE workflow_id = $workflowId AND key = $workflowInstanceKey),
               EXISTS(SELECT 1 FROM workflow_result WHERE workflow_id = $workflowId AND workflow_instance_key = $workflowInstanceKey)
      """.query[(Boolean, Boolean)].unique
    } match {
      case (false, _) => throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey))
      case (true, completed) => completed
    }
  }

  override def isWorkflowInstanceCreated(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey): Boolean = {
    runSync {
      sql"SELECT EXISTS(SELECT 1 FROM workflow_instance WHERE workflow_id = $workflowId AND key = $workflowInstanceKey)"
        .query[Boolean]
        .unique
    }
  }

  override def getWorkflowResult[Out](workflow: Workflow[?, Out], workflowInstanceKey: WorkflowInstanceKey)(using Cacheable[Out]): Option[Out] = {
    val workflowId = workflow.meta.workflowId
    runSync {
      sql"""
        SELECT wi.key, wr.result
        FROM workflow_instance wi
        LEFT JOIN workflow_result wr ON wi.workflow_id = wr.workflow_id AND wi.key = wr.workflow_instance_key
        WHERE wi.workflow_id = $workflowId AND wi.key = $workflowInstanceKey
      """.query[(WorkflowInstanceKey, Option[Array[Byte]])].option
    } match {
      case None => throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey))
      case Some((_, None)) => None
      case Some((_, Some(bytes))) => Some(Cacheable[Out].deserialize(IArray.unsafeFromArray(bytes)))
    }
  }

  override def scheduleWakeupOnTimer(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, wakeupTime: Instant): Unit = {
    val awaiterId = UUID.randomUUID()
    val now = Instant.now(clk)
    try {
      runSync {
        sql"""
          INSERT INTO workflows_awaiting_timer (awaiter_id, workflow_id, workflow_instance_key, restart_after, created_at)
          VALUES ($awaiterId, $workflowId, $workflowInstanceKey, $wakeupTime, $now)
        """.update.run
      }
      ()
    } catch {
      case e: PSQLException if e.getSQLState == "23503" =>
        throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey), Some(e))
    }
  }

  override def scheduleWakeupOnSignal(workflowId: WorkflowId, workflowInstanceKey: WorkflowInstanceKey, signal: Signal[?]): Unit = {
    val awaiterId = UUID.randomUUID()
    val now = Instant.now(clk)
    try {
      runSync {
        sql"""
          INSERT INTO workflows_awaiting_signal (awaiter_id, workflow_id, workflow_instance_key, signal_id, created_at)
          VALUES ($awaiterId, $workflowId, $workflowInstanceKey, ${signal.meta.id}, $now)
        """.update.run
      }
      ()
    } catch {
      case e: PSQLException if e.getSQLState == "23503" && e.getMessage.contains("\"workflow_signals\"") =>
        // Signal has not been set yet. The schedule is only a recovery hint; the signal store is the source of truth.
        ()
      case e: PSQLException if e.getSQLState == "23503" =>
        throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey), Some(e))
    }
  }

  override def scheduleWakeupOnWorkflowCompletion(
                                                   workflowId: WorkflowId,
                                                   workflowInstanceKey: WorkflowInstanceKey,
                                                   awaitedWorkflowId: WorkflowId,
                                                   awaitedWorkflowInstanceKey: WorkflowInstanceKey
                                                 ): Unit = {
    val awaiterId = UUID.randomUUID()
    val now = Instant.now(clk)
    try {
      runSync {
        sql"""
          INSERT INTO workflows_awaiting_workflow (awaiter_id, workflow_id, workflow_instance_key, awaited_workflow_id, awaited_workflow_instance_key, created_at)
          VALUES ($awaiterId, $workflowId, $workflowInstanceKey, $awaitedWorkflowId, $awaitedWorkflowInstanceKey, $now)
        """.update.run
      }
      ()
    } catch {
      case e: PSQLException if e.getSQLState == "23503" =>
        throw new WorkflowNotFoundException(workflowId, Some(workflowInstanceKey), Some(e))
    }
  }

  @throws[SignalConflictException]
  override def setSignal[A](
                             signal: Signal[A],
                             value: A,
                             ttl: FiniteDuration
                           )(using WorkflowInstanceMeta): Unit =
    DbSignalStore.setSignalValue(signal, value, ttl)

  override def getFingerprinter: Fingerprinter = Sha256Fingerprinter

  override def getStepIdempotencyStore(using stepCtx: StepContext[?]): StepIdempotencyStore = {
    new DbStepIdempotencyStore(stepCtx.workflowCtx.stepIdempotencyIdOverrides)(using stepCtx)
  }

  override def getStepCache[StepOut: Cacheable](using stepCtx: StepContext[StepOut]): StepCache[StepOut] = {
    new DbStepCache[StepOut]
  }

  override def getSignalStore: SignalStore = DbSignalStore

  private class DbStepIdempotencyStore(
                                        stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                                      )(
                                        using ctx: StepContext[?]
                                      ) extends StepIdempotencyStore {
    private val workflowId = ctx.workflowCtx.workflowInstanceMeta.workflowId
    private val workflowInstanceKey = ctx.workflowCtx.workflowInstanceMeta.workflowInstanceKey

    override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE library_version = $libraryVersion AND
              workflow_id = $workflowId AND
              workflow_instance_key = $workflowInstanceKey AND
              step_id = ${ctx.meta.stepId} AND
              step_version = ${ctx.meta.stepVersion} AND
              input_fingerprints = $inputFingerprints
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_key, step_id, step_version, input_fingerprints, is_only_once)
          VALUES ($id, $libraryVersion, $workflowId, $workflowInstanceKey, ${ctx.meta.stepId}, ${ctx.meta.stepVersion}, $inputFingerprints, false)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      runSync {
        idQuery.flatMap {
          case Some(existing) => Monad[ConnectionIO].pure(existing)
          case None =>
            val stepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            insertQuery(stepIdempotencyId)
        }
      }
    }

    override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE workflow_id = $workflowId AND
              workflow_instance_key = $workflowInstanceKey AND
              step_id = ${ctx.meta.stepId} AND
              is_only_once = true AND
              is_overridden = false
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_key, step_id, is_only_once)
          VALUES ($id, $libraryVersion, $workflowId, $workflowInstanceKey, ${ctx.meta.stepId}, true)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      val updateQuery: ConnectionIO[Unit] =
        sql"""
          UPDATE step_idempotency
          SET is_overridden = true
          WHERE workflow_id = $workflowId AND
                workflow_instance_key = $workflowInstanceKey AND
                step_id = ${ctx.meta.stepId} AND
                is_only_once = true AND
                is_overridden = false
        """.update.run.void

      runSync {
        idQuery.flatMap {
          case Some(existing) =>
            stepIdempotencyIdOverrides.get(ctx.meta.stepId) match {
              case Some(overrideId) if overrideId != existing =>
                updateQuery >> insertQuery(overrideId)
              case _ =>
                Monad[ConnectionIO].pure(existing)
            }
          case None =>
            val stepIdempotencyId = stepIdempotencyIdOverrides.getOrElse(
              ctx.meta.stepId,
              StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            )
            insertQuery(stepIdempotencyId)
        }
      }
    }
  }

  private class DbStepCache[Out: Cacheable](using ctx: StepContext[Out]) extends StepCache[Out] {
    private val workflowId = ctx.workflowCtx.workflowInstanceMeta.workflowId
    private val workflowInstanceKey = ctx.workflowCtx.workflowInstanceMeta.workflowInstanceKey

    override def get(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints
                    ): Option[Out] = {
      val query = sql"""
        SELECT output, step_version, input_fingerprints FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId} AND step_id = ${ctx.meta.stepId}
      """.query[(Array[Byte], Long, StepInputFingerprints)].option

      runSync(query).flatMap {
        case (data, version, fingerprints) if version == ctx.meta.stepVersion && fingerprints == inputFingerprints =>
          Some(Cacheable[Out].deserialize(IArray.unsafeFromArray(data)))
        case _ => throw new StepInputConflictException()
      }
    }

    override def put(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints,
                      value: Out,
                      ttl: Option[FiniteDuration]
                    ): Unit = {
      val expiry = ttl.map(t => Instant.now(clk).plusMillis(t.toMillis))
      val data = Cacheable[Out].serialize(value).asInstanceOf[Array[Byte]]

      val query =
        sql"""
          INSERT INTO step_cache (step_idempotency_id, step_id, step_version, input_fingerprints, output, expiry)
          VALUES (${stepIdempotencyId}, ${ctx.meta.stepId}, ${ctx.meta.stepVersion}, $inputFingerprints, $data, $expiry)
          ON CONFLICT (step_idempotency_id) DO UPDATE
          SET step_version = ${ctx.meta.stepVersion},
              input_fingerprints = $inputFingerprints,
              output = $data,
              expiry = $expiry
        """.update.run.void

      runSync(query)
    }
  }

  private object DbSignalStore extends SignalStore {

    private def select[A](signal: Signal[A])(using workflowCtx: WorkflowInstanceMeta): ConnectionIO[Option[(Array[Byte], Option[Instant])]] =
      sql"""
        SELECT value, expiry FROM workflow_signals
        WHERE id = ${signal.meta.id} AND workflow_id = ${workflowCtx.workflowId} AND workflow_instance_key = ${workflowCtx.workflowInstanceKey}
      """.query[(Array[Byte], Option[Instant])].option

    override def getSignalValue[A](signal: Signal[A])(using workflowCtx: WorkflowInstanceMeta): Option[A] =
      runSync {
        select(signal)
      }.flatMap {
        case (bytes, Some(expiry)) if Instant.now(clk).isAfter(expiry) => None
        case (bytes, _) => Some(signal.asCacheable.deserialize(IArray.unsafeFromArray(bytes)))
      }

    @throws[SignalConflictException]
    override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using workflowCtx: WorkflowInstanceMeta): Unit = {
      val expiry = Instant.now(clk).plusMillis(ttl.toMillis)
      val bytes: Array[Byte] = signal.asCacheable.serialize(value).asInstanceOf[Array[Byte]]

      try {
        runSync {
          select(signal).flatMap {
            case Some((prevBytes, _)) if java.util.Arrays.equals(prevBytes, bytes) =>
              Monad[ConnectionIO].unit

            case Some((_, _)) =>
              throw new SignalConflictException(signal)

            case None =>
              sql"""
                INSERT INTO workflow_signals (id, workflow_id, workflow_instance_key, value, expiry)
                SELECT ${signal.meta.id}, ${workflowCtx.workflowId}, ${workflowCtx.workflowInstanceKey}, $bytes, $expiry
                WHERE EXISTS (
                  SELECT 1
                  FROM workflow_instance AS wi
                  WHERE wi.workflow_id = ${workflowCtx.workflowId}
                    AND wi.key = ${workflowCtx.workflowInstanceKey}
                )
              """.update.run.map {
                case 0 => throw new WorkflowNotFoundException(workflowCtx)
                case 1 => ()
              }
          }
        }
      } catch {
        // 23505 is unique violation, i.e. duplicate primary key -> another value was inserted concurrently
        case e: PSQLException if e.getSQLState == "23505" => throw new SignalConflictException(signal)
      }
    }
  }
}

object DbWorkflowRuntime {
  /**
   * Creates a new [[DbWorkflowRuntime]] including all necessary schemas in the database.
   * Since this function executes DDL statements on the database, it should not be called often; Reuse
   * [[DbWorkflowRuntime]] instances instead of creating new ones where possible.
   *
   * @param ds DataSource to be used to connect to the database
   * @param awaitConnectionEc Execution context to be used while getting a new connection from the data source.
   *                          Asking for a connection from the data source can block when the connection is established
   *                          for the first time or when the connection pool is exhausted. Therefore, this execution
   *                          context should be from an IO thread pool.
   */
  def apply(ds: DataSource)(using awaitConnectionEc: ExecutionContext): WorkflowRuntime = {
    import de.lhns.doobie.flyway.BaselineMigrations.BaselineMigrationOps
    val flyway = Flyway.configure().dataSource(ds)
    val flywayInfo = flyway.load().info()
    flyway
      .withBaselineMigrate(flywayInfo)
      .validateMigrationNaming(true)
      .load()
      .migrate()

    new DbWorkflowRuntime(ds)(using awaitConnectionEc, Clock.systemUTC())
  }

  given Meta[StepIdempotencyId] = Meta[UUID].imap(uuid =>
    StepIdempotencyId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(StepIdempotencyId.unwrap(id))
  )

  given Meta[StepId] = Meta[UUID].imap(uuid =>
    StepId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(StepId.unwrap(id))
  )

  given Meta[WorkflowId] = Meta[UUID].imap(uuid =>
    WorkflowId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(WorkflowId.unwrap(id))
  )

  given Meta[SignalId] = Meta[UUID].imap(uuid =>
    SignalId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(SignalId.unwrap(id))
  )

  given Get[StepInputFingerprints] = pgDecoderGetT[StepInputFingerprints]

  given Put[StepInputFingerprints] = pgEncoderPutT[StepInputFingerprints]
}
