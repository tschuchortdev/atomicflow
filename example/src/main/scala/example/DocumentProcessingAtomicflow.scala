//noinspection ScalaWeakerAccess
package example

import atomicflow.*
import atomicflow.given_Conversion_String_A_StepInput
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import example.DocumentUploadEndpoint.ProcessingStatus
import example.DocumentUploadEndpoint.ProcessingStatus.{NoInfo, ProcessedSuccessfully}
import ox.*
import ox.flow.*
import ox.either.*
import ox.scheduling.*
import ox.resilience.*
import scala.util.boundary.*

import java.io.{FileNotFoundException, IOException}
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

class DocumentProcessingAtomicflow(val archiveDir: Path,
                                 virusCheckService: VirusCheckService,
                                 encryptionService: EncryptionService,
                                 documentUploadEndpoint: DocumentUploadEndpoint,
                                 resultReporter: ResultReporter)(using IORuntime) {
  import DocumentProcessingAtomicflow.{*, given}

  private val workflowRuntime = InMemoryWorkflowRuntime()

  val w1 = Workflow(WorkflowId("99a2866c-99c5-49b7-b0f5-ad097a3e3a78"), name = "document batch processing") { (inputFilePath: Path) =>
    val fileId = FileWithDocumentBatch.idFromFileName(inputFilePath.getFileName.toString)
    
    val fileParsed = readAndArchiveFile(inputFilePath)

    checkInterrupt()

    val perDocumentUploadResults = fileParsed.documents.mapPar { (document: DocumentFromInputFile) =>
      checkInterrupt()
      processIndividualDocument(document)
    }

    checkInterrupt()

    Step[Unit](StepId("fbe6acf8-731a-431f-8a48-01889ef3802e"), version = 0, name = "report input file processing status") {
      Step.cache("filePath" -> inputFilePath.toAbsolutePath.toString)

      resultReporter.reportResultSuccess(inputFilePath).unsafeRunSync()
    }
  }

  private def readAndArchiveFile(inputFilePath: Path)(using workflowContext: WorkflowContext[?,?]): FileWithDocumentBatch =
    Step(StepId("99f30d17-7443-4438-a68d-64fea152efdd"), version = 0, name = "read and archive file") {
      Step.cache("filePath" -> inputFilePath.toAbsolutePath.toString)
  
      val fileContent =
        try Files.readString(inputFilePath)
        catch {
          case e: IOException => ??? /* TODO handle read error */
        }
  
      val fileParsed =
        try FileWithDocumentBatch.fromString(fileContent)
        catch {
          case e: IllegalArgumentException => ??? /* TODO handle parsing error */
        }
  
      try {
        Files.createDirectories(archiveDir)
        Files.move(inputFilePath, archiveDir.resolve(inputFilePath.getFileName))
      }
      catch {
        case e: IOException => ??? /* TODO handle archiving error */
      }
  
      fileParsed
    }
    
  private def processIndividualDocument(document: DocumentFromInputFile)(using WorkflowContext[?, ?]) = {
    par(
      Step[Unit](StepId("a8bcb8c2-d1c9-484b-8d7f-f35c123143f7"), version = 0, name = "virus check 1") {
        Step.cache("documentContent" -> document.content)

        if (!virusCheckService.checkForVirus1(document.content).unsafeRunSync())
          throw ???
      },
      Step[Unit](StepId("9b373f01-1001-49f1-964c-72950e19b2f3"), version = 0, name = "virus check 2") {
        Step.cache("documentContent" -> document.content)

        if (!virusCheckService.checkForVirus2(document.content).unsafeRunSync())
          throw ???
      }
    )

    checkInterrupt()

    // TODO: retry signing and upload
    {
      // TODO: interrupt when signature no longer valid
      val documentSigned = Step[Array[Byte]](StepId("533dddc7-d355-43b4-81d8-bd8051808ec5"), version = 0, name = "sign document") {
        Step.cacheFor(EncryptionService.signatureValidityPeriod - 2.seconds)(
          "documentId" -> document.documentId,
          "documentContent" -> document.content
        )

        encryptionService.signDocument(document.content).unsafeRunSync()
      }

      checkInterrupt()

      Step[Unit](StepId("2a191c8b-3861-4600-ae3a-f1f8bde21db5"), version = 0, name = "upload document") {
        Step.cache("documentId" -> document.documentId, "documentSigned" -> documentSigned)

        timeout(EncryptionService.signatureValidityPeriod) {
          documentUploadEndpoint.uploadDocumentForProcessing(document.documentId, documentSigned).unsafeRunSync()
        }
      }
    }

    checkInterrupt()

    val processingResult = Step(StepId("070a7323-dd33-483c-907a-e7dd1aed2d7f"), version = 0, name = "check upload processing status") {
      Step.cache("documentId" -> document.documentId)

      retryEither(Schedule.fixedInterval(15.minutes).maxAttempts(10)) {
        (try documentUploadEndpoint.checkUploadProcessingStatus(document.documentId).unsafeRunSync()
        catch {
          case e: IOException => ProcessingStatus.NoInfo
        }) match {
          case ProcessingStatus.NoInfo => Left(ProcessingStatus.NoInfo)
          case ps: ProcessingStatus.ProcessedSuccessfully.type => Right(Right(ps))
          case ps: ProcessingStatus.ProcessedWithErrors => Right(Left(ps))
        }
      }
    }

    processingResult
  }
}
object DocumentProcessingAtomicflow {
  protected given Cacheable[FileWithDocumentBatch] = Cacheable.MsgPack.derived[FileWithDocumentBatch]

  sealed abstract class Error extends RuntimeException
  object Error {
    case class ReadingFileFailed(msg: String) extends Error
    case class ParsingFileFailed(msg: String) extends Error
    case class ArchivingFileFailed(msg: String) extends Error

    case class VirusCheckFailed(msg: String) extends Error

    case class UploadProcessedWithErrors(msg: String) extends Error
    object UploadStatusTimeoutExceeded extends Error
    object CancelledByUser extends Error
  }
}