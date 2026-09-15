package example

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.PostgresWorkflowRuntime
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.circe.syntax.*
import munit.FunSuite
import org.flywaydb.core.Flyway
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext

/** End-to-end smoke test for the document-processing example: one Postgres 16
  * container, a single Flyway migration, and a small in-memory collaborator
  * setup that processes a two-document batch to completion.
  */
class DocumentProcessingAtomicflowSuite extends FunSuite {

  private final class Pg extends PostgreSQLContainer[Pg]("postgres:16")

  private val container: Pg =
    new Pg().withDatabaseName("testdb").withUsername("postgres").withPassword("postgres")

  private def newDataSource: PGSimpleDataSource = {
    val ds = new PGSimpleDataSource()
    ds.setUrl(container.getJdbcUrl)
    ds.setUser(container.getUsername)
    ds.setPassword(container.getPassword)
    ds
  }

  private lazy val flyway: Flyway =
    Flyway.configure().dataSource(newDataSource).cleanDisabled(false).load()

  override def beforeAll(): Unit = container.start()
  override def afterAll(): Unit = container.close()

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    flyway.clean()
    flyway.migrate()
  }

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer
  given ExecutionContext = ExecutionContext.global
  given IORuntime = IORuntime.global

  test("processes a small batch end-to-end") {
    val baseDir = Files.createTempDirectory("atomicflow-example")
    val archiveDir = baseDir.resolve("archive")
    val inputFile = baseDir.resolve("batch-1.json")

    val documents = Vector(
      DocumentFromInputFile("doc-1", "content-1".getBytes("UTF-8")),
      DocumentFromInputFile("doc-2", "content-2".getBytes("UTF-8"))
    )
    Files.writeString(inputFile, FileWithDocumentBatch(documents).asJson.noSpaces)

    val uploads = new ConcurrentHashMap[String, Array[Byte]]()
    val statusChecks = new AtomicInteger(0)
    val reports = new AtomicInteger(0)

    val virusCheckService = new VirusCheckService {
      override def checkForVirus1(content: Array[Byte]): IO[Boolean] = IO.pure(true)
      override def checkForVirus2(content: Array[Byte]): IO[Boolean] = IO.pure(true)
    }
    val encryptionService = new EncryptionService {
      override def signDocument(content: Array[Byte]): IO[Array[Byte]] = IO.pure(content)
    }
    val documentUploadEndpoint = new DocumentUploadEndpoint {
      override def uploadDocumentForProcessing(
          documentId: String,
          signedDocument: Array[Byte]
      ): IO[Either[DocumentUploadEndpoint.SignatureError.type, Unit]] = {
        uploads.put(documentId, signedDocument)
        IO.pure(Right(()))
      }
      override def checkUploadProcessingStatus(documentId: String): IO[DocumentUploadEndpoint.ProcessingStatus] = {
        statusChecks.incrementAndGet()
        IO.pure(DocumentUploadEndpoint.ProcessingStatus.ProcessedSuccessfully)
      }
    }
    val resultReporter = new ResultReporter {
      override def reportResultSuccess(filePath: Path): IO[Unit] = IO { reports.incrementAndGet(); () }
    }

    val flow = new DocumentProcessingAtomicflow(
      archiveDir,
      virusCheckService,
      encryptionService,
      documentUploadEndpoint,
      resultReporter,
      newDataSource
    )

    given WorkflowRuntime = flow.runtime

    val result = flow.batchWorkflow.createAndRun("batch-1", inputFile.toString)

    assertEquals(result, WorkflowRunResult.Result(2))
    assertEquals(uploads.keySet().size, 2)
    assertEquals(statusChecks.get(), 2)
    assertEquals(reports.get(), 1)
    assert(Files.exists(archiveDir.resolve("batch-1.json")), "the input file must be archived")
  }
}
