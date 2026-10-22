package test

import atomicflow.impl.db.PostgresWorkflowRuntime
import cats.effect.IO
import doobie.ConnectionIO
import doobie.Transactor
import doobie.implicits.*
import munit.FunSuite
import org.postgresql.ds.PGSimpleDataSource

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import java.time.Clock

/** Shared database setup for PostgreSQL runtime tests.
  *
  * Hands out a suite-specific database from the JVM-wide container in
  * [[SharedPostgres]] and empties all tables before each test; the schema
  * itself is never recreated. `newRuntime` uses the same database.
  */
abstract class PostgresWorkflowRuntimeSuite extends FunSuite {

  private lazy val dataSource: PGSimpleDataSource = SharedPostgres.newDatabase()

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    SharedPostgres.truncateAll(dataSource)
  }

  private def newDataSource: PGSimpleDataSource = dataSource

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](newDataSource, ExecutionContext.global)

  /** Runs a doobie `ConnectionIO` program synchronously against the suite's DB. */
  protected def run[A](program: ConnectionIO[A]): A =
    program.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)

  /** A fresh runtime backed by the suite's single container. */
  def newRuntime: PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource)(using ExecutionContext.global)

  /** A raw JDBC connection to the suite's container, for tests that need to
    * manipulate row locks outside the runtime (e.g. hold a child's row with
    * `FOR UPDATE`).
    */
  protected def rawConnection: java.sql.Connection =
    newDataSource.getConnection

  /** A fresh runtime with an injected clock, backed by the suite's single container. */
  def newRuntime(clock: Clock): PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource, clock)(using ExecutionContext.global)

  /** A fresh runtime with injected clock, lease settings, and durable-retry
    * threshold (so tests can force inline vs. durable retries deterministically).
    */
  def newRuntime(
      clock: Clock,
      leaseDuration: FiniteDuration = 5.minutes,
      leaseAcquireTimeout: FiniteDuration = 30.seconds,
      durableRetryThreshold: FiniteDuration = 30.seconds
  ): PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource, clock, leaseDuration, leaseAcquireTimeout, durableRetryThreshold)(using
      ExecutionContext.global
    )
}
