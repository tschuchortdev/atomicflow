package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.PostgresWorkflowRuntime
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import doobie.implicits.*
import doobie.postgres.implicits.*
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.jdk.CollectionConverters.*

class UnconsumedSignalsSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  test("unconsumed signals delivered per key after cursors; consumed and unsent keys absent; payloads in sequenceId order") {
    val rt = newRuntime
    val signalA = Signal[String]("A")
    val signalB = Signal[String]("B")
    val signalC = Signal[String]("C")
    var handlerMap: Map[SignalKey, Seq[String]] = Map.empty
    val wf = Workflow[String, String](id = "uc-deliver")(
      in => {
        Step.await[String]("wait", Awaitable.SignalEvent(signalB))
        "done"
      },
      onUnconsumedSignals = m => handlerMap = m
    )
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    signalA.send(id, "a1")(using rt)
    signalA.send(id, "a2")(using rt)
    signalB.send(id, "b1")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))

    assertEquals(handlerMap.get("A"), Some(Seq("a1", "a2")))
    assertEquals(handlerMap.get("B"), None)
    assertEquals(handlerMap.get("C"), None)
  }

  test("default handler is a no-op and completion is unaffected") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-default") { in => "done" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("the handler runs after the terminal transition commits; a send inside the handler is rejected as already-completed") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    var captured: WorkflowInstanceId = null
    var sendResult: Option[SignalSendResult] = None
    val wf = Workflow[String, String](id = "uc-flip")(
      in => "done",
      onUnconsumedSignals = _ => {
        sendResult = Some(signal.send(captured, "late")(using rt))
      }
    )
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    captured = id
    signal.send(id, "x")(using rt)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(sendResult, Some(SignalSendResult.InstanceAlreadyCompleted))
  }

  test("after completion the instance's Signal events are deleted; WorkflowCompleted remains; TimerFired survives") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-delete") { in => "done" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)
    run(
      sql"""INSERT INTO workflow_events (sequence_id, event_kind, workflow_id, workflow_instance_key, workflow_instance_scope, event_key, payload, created_at)
            VALUES (nextval('workflow_event_sequence'), 'TimerFired', ${wf.id}, 'k', '{}', 'timer1', '', now())""".update.run
    )

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))

    val counts = run(
      sql"""SELECT event_kind, count(*) FROM workflow_events
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
            GROUP BY event_kind""".query[(String, Int)].to[Vector]
    ).toMap
    assertEquals(counts.getOrElse("Signal", 0), 0)
    assertEquals(counts.getOrElse("WorkflowCompleted", 0), 1)
    assertEquals(counts.getOrElse("TimerFired", 0), 1)
  }

  test("failure path still runs the handler") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    var handlerCalled = false
    val wf = Workflow[String, String](id = "uc-fail")(
      in => throw new RuntimeException("boom"),
      onUnconsumedSignals = _ => handlerCalled = true
    )
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)

    intercept[RuntimeException] { rt.runWorkflowInstance(wf, id) }
    assert(handlerCalled)
  }

  test("a signal sent to a created-but-never-started instance is deleted and WARN-logged when cancel finalizes it without a definition") {
    val rt = newRuntime
    val signal = Signal[String]("A")
    val wf = Workflow[String, String](id = "uc-finalize") { in => "done" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    signal.send(id, "x")(using rt)

    val appender = new ListAppender[ILoggingEvent]()
    appender.start()
    val logger = LoggerFactory.getLogger(classOf[PostgresWorkflowRuntime]).asInstanceOf[ch.qos.logback.classic.Logger]
    logger.addAppender(appender)
    try {
      rt.cancel(id)
    } finally {
      logger.detachAppender(appender)
    }

    val warnings = appender.list.asScala.filter(_.getLevel == Level.WARN).map(_.getFormattedMessage)
    assert(
      warnings.exists(_.contains("unconsumed signals")),
      s"expected a WARN about dropped unconsumed signals, got: $warnings"
    )

    val counts = run(
      sql"""SELECT event_kind, count(*) FROM workflow_events
            WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
            GROUP BY event_kind""".query[(String, Int)].to[Vector]
    ).toMap
    assertEquals(counts.getOrElse("Signal", 0), 0, "the finalized instance's directly addressed Signal events are deleted")
    assertEquals(counts.getOrElse("WorkflowCompleted", 0), 1)
    assertEquals(signal.send(id, "late")(using rt), SignalSendResult.InstanceAlreadyCompleted)
  }
}
