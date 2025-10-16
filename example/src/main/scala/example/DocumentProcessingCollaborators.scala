package example

import cats.effect.IO

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class FileWithDocumentBatch(fileId: String, documentsInFile: Vector[DocumentFromInputFile])
class DocumentFromInputFile(documentId: String, content: Array[Byte])

class VirusCheckService {
  def checkForVirus1: IO[Boolean] = ???
  def checkForVirus2: IO[Boolean] = ???
}

class EncryptionService {
  def signDocument(document: Array[Byte]): IO[Array[Byte]] = ???
}
object EncryptionService {
  val signatureValidityPeriod = 90.minutes
}

class DocumentUploadEndpoint {
  import DocumentUploadEndpoint.*
  def uploadDocumentForProcessing(documentId: String, signedDocument: Array[Byte]): IO[Either[SignatureError.type, Unit]] = ???
  def checkUploadProcessingStatus(documentId: String): IO[ProcessingStatus] = ???
}
object DocumentUploadEndpoint {
  object SignatureError
  
  enum ProcessingStatus {
    case NoInfo
    case ProcessedSuccessfully
    case ProcessedWithErrors
  }
}

class ResultReporter {
  def reportResultSuccess(): IO[Unit] = ???
  def reportCancellation(): IO[Unit] = ???
  def reportResultError(): IO[Unit] = ???
}