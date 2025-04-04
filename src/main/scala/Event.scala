case class Event(
                tpe: String,
                
                )

case class EventLog(events: Seq[Event])
