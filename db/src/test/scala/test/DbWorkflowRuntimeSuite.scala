package test

import atomicflow.WorkflowRuntime
import atomicflow.impl.db.DbWorkflowRuntime
import cats.effect.IO
import doobie.util.transactor.Transactor
import munit.AnyFixture
import org.flywaydb.core.Flyway
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

import scala.annotation.nowarn

import scala.concurrent.ExecutionContext

class DbWorkflowRuntimeSuite extends WorkflowRuntimeSuite {

  private class MyPgContainer extends PostgreSQLContainer[MyPgContainer]("postgres:14")


  val db = new Fixture[PostgreSQLContainer[?]]("db") {
    private var container: MyPgContainer = new MyPgContainer()
        .withDatabaseName("testdb")
        .withUsername("postgres")
        .withPassword("postgres")

    override def apply(): PostgreSQLContainer[?] = container

    override def beforeAll(): Unit = {
      super.beforeAll()
      container.start()
    }

    override def beforeEach(context: BeforeEach): Unit = {
      super.beforeEach(context)

      Flyway.configure()
        .dataSource({
          val ds = new PGSimpleDataSource()
          ds.setUrl(container.getJdbcUrl)
          ds.setUser(container.getUsername);
          ds.setPassword(container.getPassword);
          ds
        })
        .cleanDisabled(false)
        .load()
        .clean()
    }

    override def afterAll(): Unit = {
      container.close()
      super.afterAll()
    }
  }

  override def munitFixtures: Seq[AnyFixture[?]] = super.munitFixtures ++ Seq(db)

  override def createWorkflowRuntime: WorkflowRuntime = DbWorkflowRuntime({
      val ds = new PGSimpleDataSource()
      ds.setUrl(db().getJdbcUrl)
      ds.setUser(db().getUsername);
      ds.setPassword(db().getPassword);
      ds
    })(using awaitConnectionEc = ExecutionContext.global)
}
