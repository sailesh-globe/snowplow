package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters

import java.util.concurrent.TimeUnit

import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.typesafe.config.ConfigFactory
import org.specs2.Specification
import org.specs2.specification.After

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class AdapterRegistrySpec extends Specification {
  def is = sequential ^ s2"""
                This is a specification to test some AdapterRegistry functionality.
                AdapterRegistry should be able to create RemoteAdapters from a string config     ${testWrapperLocal(e1)}
                AdapterRegistry should be able to create RemoteAdapters from a resource config   ${testWrapperLocal(e2)}
                AdapterRegistry should be able to create RemoteAdapters from a file config       ${testWrapperLocal(e3)}
  """

  object testWrapperLocal extends After {
    def after =
      if (AdapterRegistry.EnrichActorSystem.isDefined) {
        AdapterRegistry.EnrichActorSystem.get.terminate()
        Await.result(AdapterRegistry.EnrichActorSystem.get.whenTerminated, Duration(5, TimeUnit.SECONDS))
      }
  }

  val vendor            = "com.blarg"
  val version           = "v1"
  val url               = "akka.tcp:remoteTestSystem@127.0.0.1:8995/user/testActor"
  val timeout           = "4s"
  val ourTestConfigFile = "RemoteAdapters.conf"

  def e1 = {

    val testConfig =
      s"""
         |akka{actor{provider:local}}
         |remoteAdapters:[ {vendor:\"$vendor\", version:\"$version\", url:\"$url\", timeout:\"$timeout\"} ]
        """.stripMargin

    commonValidation(AdapterRegistry.createRemoteAdaptersFromConfigString(testConfig))
  }

  def e2 = {

    val testConfig = ConfigFactory.parseResources(this.getClass, ourTestConfigFile)

    commonValidation(AdapterRegistry.createRemotes(testConfig))
  }

  def e3 = {
    val classPath = this.getClass.getPackage.getName.replace(".", "/")
    commonValidation(
      AdapterRegistry.createRemoteAdaptersFromConfigFile(s"src/test/resources/$classPath/$ourTestConfigFile"))
  }

  def commonValidation(parsedResults: Map[(String, String), RemoteAdapter]) = {
    val theAdapter = parsedResults.get((vendor, version))

    (
      parsedResults must haveSize(1)
        and (theAdapter must beSome[RemoteAdapter])
        and (theAdapter.get.timeout.toMillis must beEqualTo(4000))
        and (theAdapter.get.remoteUrl must beEqualTo(url))
    )
  }
}
