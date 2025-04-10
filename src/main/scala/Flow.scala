import java.time.Instant
import java.util.UUID
import scala.util.Try

// Basistyp für Logging
case class StepEvent(
                      name: String,
                      inputHash: String,
                      uuid: Option[String] = None,
                      output: Option[Any] = None,
                      timestamp: Instant = Instant.now()
                    )

trait EventLog {
  def load(name: String, inputHash: String, uuid: Option[String] = None): Option[StepEvent]

  def save(event: StepEvent): Unit
}

trait FixedPointLog {
  def getOrAssignUUID(effectName: String, instanceId: Option[String], inputHash: String): UUID

  def overrideUUID(effectName: String, instanceId: Option[String], inputHash: String, newUUID: UUID): Unit
}

trait OutputCache {
  
}

// Context für die Ausführung
case class WorkflowContext(
                            instanceUuid: String,
                            eventLog: EventLog,
                            outputCache: OutputCache,
                            fixedPointLog: FixedPointLog
                          ) {
  def hash(input: Any): String = input.hashCode().toString // Bessere Hash-Funktion optional
}

def step[I, O](name: String)(f: I => O)(input: I)(using ctx: WorkflowContext): O = {
  val hash = ctx.hash(input)
  ctx.eventLog.load(name, hash) match {
    case Some(event) => event.output.get.asInstanceOf[O]
    case None =>
      val result = f(input)
      ctx.eventLog.save(StepEvent(name, hash, output = Some(result)))
      result
  }
}

def fixedPointEffect[I, O](name: String, input: I, uuid: String)
                          (effect: I => O)
                          (using ctx: WorkflowContext): O = {
  val hash = ctx.hash(input)
  ctx.eventLog.load(name, hash, Some(uuid)) match {
    case Some(event) =>
      // Prüfen, ob Hash noch gleich ist (Replay erlaubt)
      if (ctx.hash(input) != event.inputHash)
        throw new IllegalStateException(s"Hash mismatch on fixedPointEffect '$name'. UUID reused with different input.")
      event.output.get.asInstanceOf[O]
    case None =>
      val result = effect(input)
      ctx.eventLog.save(StepEvent(name, hash, Some(uuid), Some(result)))
      result
  }
}

def expiringStep[I, O](name: String, ttlSeconds: Long)(f: I => O)(input: I)(using ctx: WorkflowContext): O = {
  val hash = ctx.hash(input)
  ctx.eventLog.load(name, hash) match {
    case Some(event) if event.timestamp.plusSeconds(ttlSeconds).isAfter(Instant.now()) =>
      event.output.get.asInstanceOf[O]
    case _ =>
      val result = f(input)
      ctx.eventLog.save(StepEvent(name, hash, output = Some(result)))
      result
  }
}

class InMemoryEventLog extends EventLog {
  private val store = scala.collection.mutable.Map.empty[(String, String, Option[String]), StepEvent]

  def load(name: String, inputHash: String, uuid: Option[String] = None): Option[StepEvent] =
    store.get((name, inputHash, uuid))

  def save(event: StepEvent): Unit =
    store((event.name, event.inputHash, event.uuid)) = event
}

class InMemoryFixedPointLog extends FixedPointLog {
  private val store = scala.collection.mutable.Map.empty[(String, String), UUID]

  def getOrAssignUUID(effectName: String, inputHash: String): UUID = {
    store.getOrElseUpdate((effectName, inputHash), UUID.randomUUID())
  }

  def overrideUUID(effectName: String, inputHash: String, newUUID: UUID): Unit = {
    store((effectName, inputHash)) = newUUID
  }

  override def getOrAssignUUID(effectName: String, instanceId: Option[String], inputHash: String): UUID = ???

  override def overrideUUID(effectName: String, instanceId: Option[String], inputHash: String, newUUID: UUID): Unit = ???
}

def sendEmail(email: String): String = {
  println(s"📤 Sending email to $email")
  s"sent-to:$email"
}

def withWorkflow[A](f: WorkflowContext ?=> A): A = {
  given ctx: WorkflowContext = WorkflowContext("", new InMemoryEventLog, new OutputCache {}, new InMemoryFixedPointLog)
  f
}
/*
withWorkflow {
  val a = step("A") { in =>
    ""
  }

  val b = step("B") {in =>
    in
  }(a)

  b
}
*/
def runWorkflow(input: String, uuid: String)(using ctx: WorkflowContext): String = {
  val enriched = step("Enrich")((in: String) => in.trim.toUpperCase)(input)
  val _ = fixedPointEffect("SendEmail", enriched, uuid) { email => sendEmail(email) }
  "done"
}

given ctx: WorkflowContext = WorkflowContext("", new InMemoryEventLog, new OutputCache {}, new InMemoryFixedPointLog)

@main def test(): Unit = {
  val uuid = UUID.randomUUID().toString
  runWorkflow("alice@example.com", uuid)
  println("Retry...")
  runWorkflow("alice@example.com", uuid) // sollte sendEmail NICHT erneut ausführen
}
