package com.example

import akka.actor.{ActorSystem, Props}

/**
 * This app is a simplified example of an Enrich Remote Adapter.
 *
 * It can also be used as-is to validate the External integration tests of the scala-common-enrich RemoteAdapterSpec test class:
 * if you set the externalActorUrl in that test class to Some("akka.tcp://remoteTestSystem@127.0.0.1:8995/user/testActor"),
 * then the External tests in that class should pass whenever this app is running.
 */
object RemoteAdapterExample extends App {

  val ActorSystemName = "remoteTestSystem"
  val ActorName       = "testActor"

  val actorSystem = ActorSystem(ActorSystemName)

  actorSystem.actorOf(Props(new MockAdapter("igluResolver.conf")), ActorName)
}
