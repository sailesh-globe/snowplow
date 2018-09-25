package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.specs2.Specification
import org.specs2.specification.After

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RemoteAdaptersSpec extends Specification {
  def is = sequential ^ s2"""
                This is a specification to test some AdapterRegistry functionality.
                AdapterRegistry should be able to create RemoteAdapters from a string config     ${testWrapperLocal(e1)}
                AdapterRegistry should be able to create RemoteAdapters from a resource config   ${testWrapperLocal(e2)}
                AdapterRegistry should be able to create RemoteAdapters from a file config       ${testWrapperLocal(e3)}
  """

  object testWrapperLocal extends After {
    def after =
      if (RemoteAdapters.EnrichActorSystem.isDefined) {
        RemoteAdapters.EnrichActorSystem.get.terminate()
        Await.result(RemoteAdapters.EnrichActorSystem.get.whenTerminated, Duration(5, TimeUnit.SECONDS))
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

    val actual = RemoteAdapters.createFromConfigString(testConfig)
    commonValidation(actual)
  }

  def e2 = {

    val testConfig = ConfigFactory.parseResources(this.getClass, ourTestConfigFile)

    val actual = RemoteAdapters.createFromConfig(testConfig)
    commonValidation(actual)
  }

  def e3 = {
    val classPath = this.getClass.getPackage.getName.replace(".", "/")

    val actual = RemoteAdapters.createFromConfigFile(s"src/test/resources/$classPath/$ourTestConfigFile")
    commonValidation(actual)
  }

  def commonValidation(actual: Map[(String, String), RemoteAdapter]) = {
    val theAdapter = actual.get((vendor, version))

    (
      theAdapter must beSome[RemoteAdapter]
        and (theAdapter.get.timeout.toMillis must beEqualTo(4000))
        and (theAdapter.get.remoteUrl must beEqualTo(url))
    )
  }
}
