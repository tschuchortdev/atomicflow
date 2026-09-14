package test

import atomicflow.impl.db.PostgresWorkflowRuntime
import cats.effect.IO
import doobie.ConnectionIO
import doobie.Transactor
import doobie.implicits.*
import munit.FunSuite
import org.flywaydb.core.Flyway
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import java.time.Clock

/** Shared munit harness for the Postgres backend.
  *
  * Starts ONE PostgreSQL 16 container for the whole suite, applies the Flyway
  * migrations once, and cleans all tables between tests (Flyway clean +
  * migrate). `newRuntime` returns a fresh runtime backed by the same container.
  */
abstract class PostgresWorkflowRuntimeSuite extends FunSuite {

  private final class Pg extends PostgreSQLContainer[Pg]("postgres:16")

  private val container: Pg =
    new Pg().withDatabaseName("testdb").withUsername("postgres").withPassword("postgres")

  override def beforeAll(): Unit = container.start()

  override def afterAll(): Unit = container.close()

  private def newDataSource: PGSimpleDataSource = {
    val ds = new PGSimpleDataSource()
    ds.setUrl(container.getJdbcUrl)
    ds.setUser(container.getUsername)
    ds.setPassword(container.getPassword)
    ds
  }

  private lazy val flyway: Flyway =
    Flyway.configure().dataSource(newDataSource).cleanDisabled(false).load()

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    flyway.clean()
    flyway.migrate()
  }

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](newDataSource, ExecutionContext.global)

  /** Runs a doobie `ConnectionIO` program synchronously against the suite's DB. */
  protected def run[A](program: ConnectionIO[A]): A =
    program.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)

  /** A fresh runtime backed by the suite's single container. */
  def newRuntime: PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource)(using ExecutionContext.global)

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
