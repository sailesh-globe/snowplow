package com.example

import akka.actor.{Actor, ActorLogging}
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.ValidatedRawEvents
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse
import scalaz.Scalaz._
import scalaz.{Failure, Success}

class MockAdapter(resolverConfFilename: String) extends Adapter with Actor with ActorLogging {

  val mockTracker  = "testTracker-v0.1"
  val mockPlatform = "srv"
  val mockSchemaKey        = "moodReport"
  val mockSchemaVendor     = "org.remoteActorTest"
  val mockSchemaName       = "moodChange"

  val bodyMissingErrorText = "missing payload body"
  val emptyListErrorText   = "no events were found in payload body"
  val doubleErrorText      = List("error one", "error two")

  private val EventSchemaMap = Map(
    mockSchemaKey -> SchemaKey(mockSchemaVendor, mockSchemaName, "jsonschema", "1-0-0").toSchemaUri
  )

  implicit val resolver = IgluUtils.getResolver(resolverConfFilename)

  override def preStart(): Unit = log.info("MockListener started")

  override def postStop(): Unit = log.info("MockListener stopped")

  override def receive = {

    case payload: CollectorPayload =>
      val parsedEvents = toRawEvents(payload)

      parsedEvents match {
        case Success(events) => sender() ! Right(events.head :: events.tail)
        case Failure(msgs)   => sender() ! Left(msgs.head :: msgs.tail)
      }
  }

  override def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    if (payload.body.isEmpty) {
      bodyMissingErrorText.failNel

    } else if (payload.body.get == "") {
      doubleErrorText.toNel.get.fail

    } else {
      parse(payload.body.get) \ "mood" match {
        case JArray(list) =>
          val schema = lookupSchema(mockSchemaKey.some, "Listener", 0, EventSchemaMap)

          val events = list.map { event =>
            RawEvent(
              api = payload.api,
              parameters = toUnstructEventParams(mockTracker,
                                                 toMap(payload.querystring),
                                                 schema.toOption.get,
                                                 event,
                                                 mockPlatform),
              contentType = payload.contentType,
              source      = payload.source,
              context     = payload.context
            ).success
          }

          if (events.isEmpty)
            emptyListErrorText.failNel
          else
            rawEventsListProcessor(events)

        case _ => "ng".failNel
      }
    }

}
