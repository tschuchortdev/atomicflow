import scala.util.Try

object Syntax {
  case class HashedInput(hash: String)
  case class Output(json: String)
  
  abstract sealed class Event(stepName: String)
  
  case class InEvent(stepName: String, in: ) extends Event
  
  case class Step[F[_], In, Out](name: String, f: In => F[Out]) {
    def execute(in: In, events: ): (Try[Out], Seq[Event])
  }
  
  
}
