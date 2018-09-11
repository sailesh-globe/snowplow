package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.loaders.{
  CollectorApi,
  CollectorContext,
  CollectorPayload,
  CollectorSource
}
import org.joda.time.DateTime
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers
import org.specs2.specification.BeforeAfter
import scalaz.NonEmptyList
import scalaz.Scalaz._

import scala.concurrent.duration.Duration

class RemoteAdapterSpec extends Specification with ValidationMatchers {

  override def is = sequential ^ s2"""
   This is a specification to test the RemoteAdapter functionality.
   the adapter must return any events parsed by the remote actor                 ${testWrapper(e1)}
   the remote actor must treat any empty list as an error                        ${testWrapper(e2)}
   the adapter must also return any other errors issued by the remote actor      ${testWrapper(e3)}
   """

  implicit val resolver = SpecHelpers.IgluResolver

  val mockTracker          = "testTracker-v0.1"
  val mockPlatform         = "srv"
  val mockSchemaKey        = "moodReport"
  val mockSchemaVendor     = "org.remoteActorTest"
  val mockSchemaName       = "moodChange"
  val mockSchemaFormat     = "jsonschema"
  val mockSchemaVersion    = "1-0-0"
  val bodyMissingErrorText = "missing payload body"
  val emptyListErrorText   = "no events were found in payload body"

  class TestActor extends Actor with ActorLogging with Adapter {

    private val EventSchemaMap = Map(
      mockSchemaKey -> SchemaKey(mockSchemaVendor, mockSchemaName, mockSchemaFormat, mockSchemaVersion).toSchemaUri
    )

    override def receive = {
      case payload: CollectorPayload =>
        val raws = toRawEvents(payload)
        sender() ! raws
    }

    override def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
      if (payload.body.isEmpty) {
        bodyMissingErrorText.failNel
      } else {
        parse(payload.body.get) \ "mood" match {
          case JArray(list) =>
            val schema = lookupSchema(mockSchemaKey.some, "", 0, EventSchemaMap)

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

  object Shared {
    val api       = CollectorApi("org.remoteActorTest", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
                                   "37.157.33.123".some,
                                   None,
                                   None,
                                   Nil,
                                   None)
  }

  var actorSystem: ActorSystem   = _
  var testAdapter: RemoteAdapter = _

  object testWrapper extends BeforeAfter {

    def before = {
      val systemName = "TESTSPEC"
      actorSystem = ActorSystem(systemName)
      val actor = actorSystem.actorOf(Props(new TestActor()), "testActor")

      testAdapter = new RemoteAdapter(actorSystem,
                                      s"akka://$systemName/user/testActor",
                                      Duration(5, java.util.concurrent.TimeUnit.SECONDS))
    }

    def after =
      actorSystem.terminate()
  }

  def e1 = {
    val eventData    = List(("anonymous", -0.3), ("subscribers", 0.6))
    val eventsAsJson = eventData.map(evt => s"""{"${evt._1}":${evt._2}}""")

    val payloadBody = s""" {"mood": [${eventsAsJson.mkString(",")}]} """
    val payload     = CollectorPayload(Shared.api, Nil, None, payloadBody.some, Shared.cljSource, Shared.context)

    val expected = eventsAsJson
      .map(
        evtJson =>
          RawEvent(
            Shared.api,
            Map(
              "tv"    -> mockTracker,
              "e"     -> "ue",
              "p"     -> mockPlatform,
              "ue_pr" -> s"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:$mockSchemaVendor/$mockSchemaName/$mockSchemaFormat/$mockSchemaVersion","data":$evtJson}}"""
            ),
            None,
            Shared.cljSource,
            Shared.context
        ))
      .toNel
      .get

    testAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val emptyListPayload =
      CollectorPayload(Shared.api, Nil, None, "{\"mood\":[]}".some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList(emptyListErrorText)
    testAdapter.toRawEvents(emptyListPayload) must beFailing(expected)
  }

  def e3 = {
    val bodylessPayload = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected        = NonEmptyList(bodyMissingErrorText)
    testAdapter.toRawEvents(bodylessPayload) must beFailing(expected)
  }

}
