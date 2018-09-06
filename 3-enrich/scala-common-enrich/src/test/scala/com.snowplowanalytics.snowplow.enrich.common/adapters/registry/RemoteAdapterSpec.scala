package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.snowplowanalytics.snowplow.enrich.common.loaders.{
  CollectorApi,
  CollectorContext,
  CollectorPayload,
  CollectorSource
}
import org.joda.time.DateTime
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers
import org.specs2.specification.BeforeAfterEach
import scalaz.Scalaz._
import scalaz._

class RemoteAdapterSpec extends Specification with ValidationMatchers with BeforeAfterEach {

  //TODO not working yet
  def before = println("before!!!")
  def after  = println("after!!!")

  override def is = s2"""
   This is a specification to test the RemoteAdapter functionality.
   toRawEvents must return a Failure Nel for TBD                                      $e1
   """

  implicit val resolver = SpecHelpers.IgluResolver

  var receivedPayload: Option[CollectorPayload] = None

  class TestActor extends Actor with ActorLogging {
    override def receive = {
      case payload: CollectorPayload =>
        receivedPayload = Some(payload)
      case _ =>
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

  /*
  var actorSystem: ActorSystem = _
  var adapter: RemoteAdapter = _

   def before = {
     println("beforing")
    receivedPayload = None

    val systemName  = "TESTRECEIVERSYSTEM"
    actorSystem = ActorSystem(systemName)
    val actor = actorSystem.actorOf(Props(new TestActor()), "testActor")

     println("before creating adapter")
    adapter = new RemoteAdapter(actorSystem, s"akka://$systemName/user/testActor")
     println(adapter)

  }

  def after = {
    println("aftering")
    actorSystem.terminate()
  }
   */

  def e1 = {
    val payload  = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected = "nothing to return"

    val systemName  = "TESTRECEIVERSYSTEM"
    val actorSystem = ActorSystem(systemName)

    val actor = actorSystem.actorOf(Props(new TestActor()), "testActor")

    val adapter = new RemoteAdapter(actorSystem, s"akka://$systemName/user/testActor")
    receivedPayload = None

    val events = adapter.toRawEvents(payload)
    actorSystem.terminate()

    ((receivedPayload.isDefined must beTrue)
      and (receivedPayload.get must beEqualTo(payload))
      and (events must beFailing(NonEmptyList(expected))))
  }

}
