package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

import akka.actor.ActorSystem
import akka.pattern.Patterns
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import scalaz.Scalaz._

import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
  * A client for an Akka-remoted enrich adapter.
  * @constructor create a new client actor in the given local actor system, to talk to the given remote adapter.
  * @param actorSystem the local Akka ActorSystem
  * @param remoteUrl the url of the remote adapter, e.g. akka.tcp://myRemoteActorSystem@10.1.1.1:8088/user/myEnrichAdapter
  * @param timeout max duration of the wait for each response from the remote
  */
class RemoteAdapter(actorSystem: ActorSystem, val remoteUrl: String, val timeout: FiniteDuration) extends Adapter {

  private val actorSelection = actorSystem.actorSelection(remoteUrl)

  /**
    * Send the given payload to the remote adapter,
    * wait for it to respond with an Either[List[String], List[RawEvent] ],
    * and return that as a ValidatedRawEvents
    * @param payload The CollectorPaylod containing one or more
    *        raw events as collected by a Snowplow collector
    * @param resolver (implicit) The Iglu resolver used for
    *        schema lookup and validation
    * @return a Validation boxing either a NEL of RawEvents on
    *         Success, or a NEL of Failure Strings
    */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    Await
      .result(Patterns.ask(actorSelection, payload, timeout), timeout + 1.second)
      .asInstanceOf[Either[List[String], List[RawEvent]]] match {
      case Right(events) => events.toNel.get.success
      case Left(errors)  => errors.toNel.get.fail
      case ng => s"Unexpected response from remote $remoteUrl $ng".failNel
    }

}
