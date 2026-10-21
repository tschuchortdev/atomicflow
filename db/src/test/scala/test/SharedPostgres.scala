package test

import org.flywaydb.core.Flyway
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

/** One PostgreSQL 16 container shared by every suite in this JVM, with one
  * database per suite.
  *
  * Starting a container costs several seconds, so suites share a single
  * instance instead of starting one each. Each suite gets its own database so
  * pooled connections and background workers from one suite can never touch
  * another suite's data. Suites isolate their tests by emptying all tables
  * between tests; the schema is static (one Flyway migration), so truncation
  * is equivalent to a full clean and migrate. The container is closed by a
  * JVM shutdown hook, which fires when sbt's test JVM exits.
  */
object SharedPostgres {

  private final class Pg extends PostgreSQLContainer[Pg]("postgres:16")

  private lazy val container: Pg = {
    val c = new Pg().withDatabaseName("testdb").withUsername("postgres").withPassword("postgres")
    c.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => c.close()))
    c
  }

  private def newDataSource(database: String): PGSimpleDataSource = {
    val ds = new PGSimpleDataSource()
    ds.setUrl(container.getJdbcUrl)
    ds.setDatabaseName(database)
    ds.setUser(container.getUsername)
    ds.setPassword(container.getPassword)
    ds
  }

  private val databaseCounter = new AtomicInteger(0)

  /** Starts the container if needed and hands out a fresh, migrated database. */
  def newDatabase(): PGSimpleDataSource = synchronized {
    container
    val name = s"af_test_${databaseCounter.incrementAndGet()}"
    val admin = newDataSource("postgres")
    try {
      val st = admin.getConnection.createStatement()
      try st.execute(s"""CREATE DATABASE "$name"""")
      finally st.close()
    } finally admin.getConnection.close()
    val ds = newDataSource(name)
    Flyway.configure().dataSource(ds).cleanDisabled(false).load().migrate()
    ds
  }

  /** Empties all tables and restarts identity sequences, keeping the schema in place.
    *
    * Tables that the migration seeds with singleton rows are not truncated;
    * their mutable columns are reset instead, so the seed data stays intact.
    */
  def truncateAll(ds: PGSimpleDataSource): Unit = {
    val conn: Connection = ds.getConnection
    try {
      val (seeded, plain) = {
        val rs = conn
          .createStatement()
          .executeQuery(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'flyway_schema_history'"
          )
        try {
          val seededBuilder = Vector.newBuilder[String]
          val plainBuilder  = Vector.newBuilder[String]
          while rs.next() do {
            val t = rs.getString(1)
            if seededTables.contains(t) then seededBuilder += t else plainBuilder += t
          }
          (seededBuilder.result(), plainBuilder.result())
        } finally rs.getStatement.close()
      }
      if plain.nonEmpty then {
        val ddl = plain.map(t => s"\"$t\"").mkString("TRUNCATE TABLE ", ", ", " RESTART IDENTITY CASCADE")
        val st = conn.createStatement()
        try st.execute(ddl)
        finally st.close()
      }
      for t <- seeded do {
        val st = conn.createStatement()
        try st.executeUpdate(s"UPDATE \"$t\" SET dispatched_through = 0")
        finally st.close()
      }
    } finally conn.close()
  }

  /** Migration-seeded tables whose singleton rows must survive a reset. */
  private val seededTables = Set("workflow_event_dispatch")
}
