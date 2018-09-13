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

class RemoteAdapter(actorSystem: ActorSystem, val remoteUrl: String, val timeout: FiniteDuration) extends Adapter {

  private val actorSelection = actorSystem.actorSelection(remoteUrl)

  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {

    val resultFromRemote = Await.result(Patterns.ask(actorSelection, payload, timeout), timeout + 1.second)

    resultFromRemote match {
      case events: List[RawEvent]       => events.toNel.get.success
      case errors: Option[List[String]] => errors.get.toNel.get.fail
      case _                            => s"not good, got this from remote but don't know what to do with it: $resultFromRemote".failNel
    }
  }

}
