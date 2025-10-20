package example

import io.circe.syntax.*
import io.circe.*
import cats.syntax.all.*
import cats.effect.syntax.all.*
import cats.effect.IO

import java.nio.file.Path
import java.util.Base64
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class FileWithDocumentBatch(documents: Vector[DocumentFromInputFile]) derives Codec.AsObject
object FileWithDocumentBatch {
  def idFromFileName(fileName: String) = fileName.takeWhile(_ != '.')

  def fromString(s: String) =
    io.circe.parser.parse(s).flatMap(_.as[FileWithDocumentBatch]) match {
      case Left(err) => throw IllegalArgumentException(s"Could not parse json: ${err.getMessage}")
      case Right(value) => value
    }
}

case class DocumentFromInputFile(documentId: String, content: Array[Byte]) derives Codec.AsObject

/** Serializes or deserializes a byte array as a base64-coded padded JSON string */
given byteArrayBase64Codec: Codec[Array[Byte]] = Codec.from(
  summon[Decoder[String]].map(Base64.getDecoder.decode(_)),
  summon[Encoder[String]].contramap(Base64.getEncoder.encodeToString(_))
)

class VirusCheckService {
  def checkForVirus1(content: Array[Byte]): IO[Boolean] = IO.sleep(1.second).as(true)
  def checkForVirus2(content: Array[Byte]): IO[Boolean] = IO.sleep(1.second).as(true)
}

class EncryptionService {
  def signDocument(document: Array[Byte]): IO[Array[Byte]] = document.pure[IO]
}
object EncryptionService {
  val signatureValidityPeriod = 90.minutes
}

class DocumentUploadEndpoint {
  import DocumentUploadEndpoint.*
  def uploadDocumentForProcessing(documentId: String, signedDocument: Array[Byte]): IO[Either[SignatureError.type, Unit]] =
    IO.pure(Right(()))

  def checkUploadProcessingStatus(documentId: String): IO[ProcessingStatus] =
    IO.pure(ProcessingStatus.ProcessedSuccessfully)
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
  def reportResultSuccess(filePath: Path): IO[Unit] = IO.unit
  def reportCancellation(filePath: Path): IO[Unit] = IO.unit
  def reportResultError(filePath: Path, errorMsg: String): IO[Unit] = IO.unit
}