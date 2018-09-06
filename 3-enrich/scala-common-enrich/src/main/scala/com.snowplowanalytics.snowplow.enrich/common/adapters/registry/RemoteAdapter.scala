package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

import akka.actor.ActorSystem
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import scalaz.Scalaz._

class RemoteAdapter(actorSystem: ActorSystem, remoteUrl: String) extends Adapter {

  private val actor = actorSystem.actorSelection(remoteUrl)

  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {

    actor ! payload

    "nothing to return".failureNel
  }
}
