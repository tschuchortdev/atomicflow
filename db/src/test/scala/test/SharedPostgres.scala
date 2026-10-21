package test

import org.flywaydb.core.Flyway
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** One PostgreSQL 16 container shared by every suite in this JVM, with one
  * database per suite.
  *
  * Starting a container costs several seconds, so suites share a single
  * instance instead of starting one each. Each suite gets its own database so
  * pooled connections and background workers from one suite can never touch
  * another suite's data. Suites isolate their tests by emptying all tables
  * between tests and restoring the rows the migrations seed into a fresh
  * schema; the rest of the schema is never recreated. Because every database
  * is created and migrated from scratch, migration changes are picked up on
  * the next test run automatically. The container is closed by a JVM shutdown
  * hook, which fires when sbt's test JVM exits.
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

  private val seedInsertsByDatabase = new ConcurrentHashMap[PGSimpleDataSource, Vector[String]]()

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
    seedInsertsByDatabase.put(ds, snapshotSeedInserts(ds))
    ds
  }

  /** Empties all tables and restarts identity sequences, keeping the schema in
    * place, then restores the migration-seeded rows captured at creation time.
    */
  def truncateAll(ds: PGSimpleDataSource): Unit = {
    val seeds = seedInsertsByDatabase.get(ds)
    val conn: Connection = ds.getConnection
    try {
      val tables = userTables(conn)
      if tables.nonEmpty then {
        val ddl = tables.map(t => s"\"$t\"").mkString("TRUNCATE TABLE ", ", ", " RESTART IDENTITY CASCADE")
        val st = conn.createStatement()
        try st.execute(ddl)
        finally st.close()
      }
      if seeds.nonEmpty then {
        val st = conn.createStatement()
        try seeds.foreach(st.execute)
        finally st.close()
      }
    } finally conn.close()
  }

  /** The contents of all user tables right after migration; on a fresh schema
    * these are exactly the rows seeded by the migrations.
    */
  private def snapshotSeedInserts(ds: PGSimpleDataSource): Vector[String] = {
    val conn: Connection = ds.getConnection
    try {
      userTables(conn).toVector.flatMap { table =>
        val st = conn.createStatement()
        try {
          val rs = st.executeQuery(s"""SELECT * FROM "$table"""")
          try {
            val meta = rs.getMetaData
            val columns = (1 to meta.getColumnCount).map(i => s""""${meta.getColumnName(i)}"""")
            val inserts = Vector.newBuilder[String]
            while rs.next() do {
              val values = (1 to meta.getColumnCount).map { i =>
                val v = rs.getString(i)
                if v == null then "NULL" else "'" + v.replace("'", "''") + "'"
              }
              inserts += s"""INSERT INTO "$table" (${columns.mkString(", ")}) VALUES (${values.mkString(", ")})"""
            }
            inserts.result()
          } finally rs.close()
        } finally st.close()
      }
    } finally conn.close()
  }

  private def userTables(conn: Connection): Vector[String] = {
    val rs = conn
      .createStatement()
      .executeQuery(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'flyway_schema_history'"
      )
    try {
      val b = Vector.newBuilder[String]
      while rs.next() do b += rs.getString(1)
      b.result()
    } finally rs.getStatement.close()
  }
}
