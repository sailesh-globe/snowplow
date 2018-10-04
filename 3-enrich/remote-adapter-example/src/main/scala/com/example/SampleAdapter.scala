package com.example

import com.snowplowanalytics.iglu.client.SchemaKey
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, parse}

class SampleAdapter(resolverConfFilename: String) {

  val sampleTracker = "testTracker-v0.1"
  val samplePlatform = "srv"
  val sampleSchemaKey = "moodReport"
  val sampleSchemaVendor = "org.remoteEnricherTest"
  val sampleSchemaName = "moodChange"

  val doubleErrorText = List("error one", "error two")

  implicit val formats = DefaultFormats
  implicit val resolver = IgluUtils.getResolver(resolverConfFilename)

  sealed case class Payload(
                             queryString: Map[String, String],
                             body: String,
                             contentType: String
                           )

  def handle(decodedBody: Any) =
    try {

      parse(decodedBody.asInstanceOf[String]).extract[Payload] match {
        case payload =>
          val json = compact(
            ("schema" -> SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri) ~
              ("data" -> ("schema" -> SchemaKey("com.example", "sample_event", "jsonschema", "1-0-0").toSchemaUri) ~
                ("data" -> payload.body))
          )
          Map("tv" -> sampleTracker,
            "e" -> "ue",
            "p" -> payload.queryString.getOrElse("p", samplePlatform), // Required field
            "ue_pr" -> json) ++ payload.queryString
        case anythingElse =>
          Left(List(s"expecting a payload json but got a ${anythingElse.getClass}"))
      }
    } catch {
      case e: Exception => Left(List(s"aack, sampleAdapter exception $e"))
    }
}
