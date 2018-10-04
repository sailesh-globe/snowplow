package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.softwaremill.sttp._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory
import scalaz._
import scalaz.Scalaz._

import scala.concurrent.duration.FiniteDuration

/**
 * An adapter for an enrichment that is handled by a remote webservice.
 *
 * @constructor create a new client to talk to the given remote webservice.
 * @param remoteUrl the url of the remote webservice, e.g. http://localhost/myEnrichment
 * @param timeout max duration of each connection attempt, and of the wait for each response from the remote
 */
class RemoteAdapter(val remoteUrl: String, val timeout: FiniteDuration) extends Adapter {

  val bodyMissingErrorText = "missing payload body"
  val emptyListErrorText   = "no events were found in payload body"

  private lazy val log = LoggerFactory.getLogger(getClass)

  val remoteUri = uri"$remoteUrl"
  implicit val backend = HttpURLConnectionBackend(
    options = SttpBackendOptions.connectionTimeout(timeout)
  )

  /**
   * POST the given payload to the remote webservice,
   * wait for it to respond with an Either[List[String], List[RawEvent] ],
   * and return that as a ValidatedRawEvents
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    if (payload.body.isEmpty || payload.body.get == "") {
      bodyMissingErrorText.failNel
    } else {

      val json = ("contentType" -> payload.contentType) ~
        ("queryString" -> toMap(payload.querystring)) ~
        ("body" -> payload.body)

      httpPost(remoteUrl, compact(render(json))) match {
        case Left(errmsg) =>
          errmsg.failNel
        case Right(bodyAsString) =>
          val responseJson = parse(RemoteAdapter.deserializeFromBase64(bodyAsString).asInstanceOf[String])

          val events = (responseJson \ "events").extract[List[RawEventParameters]] match {
            case rawEventParameters =>
              rawEventParameters.map { rawEventParameters =>
                RawEvent(
                  api = payload.api,
                  parameters = rawEventParameters,
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                ).success
              }
          }
          val error = (responseJson \ "error").extract[String]
          if (events.isEmpty) {
            if (error != null) {
              error.failNel
            } else {
              emptyListErrorText.failNel
            }
          } else {
            rawEventsListProcessor(events)
          }
        case ng => s"Unexpected response from remote $remoteUrl $ng".failNel
      }
    }
  }

  private def httpPost(requestUrl: String, body: String): Either[String, String] =
    try {
      val request  = sttp.readTimeout(timeout).body(body).post(uri"$requestUrl")
      val response = request.send()

      if (response.isClientError)
        Left(s"remote webservice rejected this request with a ${response.code} error! ('${response.body}')")
      else if (response.isServerError)
        Left(s"remote webservice choked on this request, got a ${response.code} error! ('${response.body}')")
      else
        Right(response.unsafeBody)

    } catch {
      case e: Exception =>
        log.error(s"Caught an exception on $requestUrl", e)
        Left(s"Caught an HTTP exception: ${e.getMessage}")
    }
}

object RemoteAdapter {

  def serializeToBase64(p: Object): String = {
    val baos = new ByteArrayOutputStream()
    val oos  = new ObjectOutputStream(baos)
    oos.writeObject(p)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }

  def deserializeFromBase64(s: String): Any = {
    val bytes = Base64.getDecoder.decode(s)
    val ois   = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val p     = ois.readObject()
    ois.close()
    p
  }

}
