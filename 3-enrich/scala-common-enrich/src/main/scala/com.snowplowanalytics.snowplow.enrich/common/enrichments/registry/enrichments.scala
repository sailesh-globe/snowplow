/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Scalaz
import java.net.URI

import scalaz._
import Scalaz._
import com.github.fge.jsonschema.core.report.ProcessingMessage
import org.json4s.JsonAST.{JObject, JString}

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.{SchemaCriterion, SchemaKey}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.ScalazJson4sUtils

/**
 * Trait inherited by every enrichment config case class
 */
trait Enrichment {

  /**
   * Gets the list of files the enrichment requires cached locally.
   * The default implementation returns an empty list; if an
   * enrichment requires files, it must override this method.
   *
   * @return A list of pairs, where the first entry in the pair
   * indicates the (remote) location of the source file and the
   * second indicates the local path where the enrichment expects
   * to find the file.
   */
  def filesToCache: List[(URI, String)] = List.empty
}

/**
 * Trait to hold helpers relating to enrichment config
 */
trait ParseableEnrichment {

  val supportedSchema: SchemaCriterion

  /**
   * Tests whether a JSON is parseable by a
   * specific EnrichmentConfig constructor
   *
   * @param config The JSON
   * @param schemaKey The schemaKey which needs
   *        to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[JValue] =
    if (supportedSchema matches schemaKey) {
      config.success
    } else {
      ("Schema key %s is not supported. A '%s' enrichment must have schema '%s'.")
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .toProcessingMessage
        .fail
        .toValidationNel
    }
}

sealed trait EventSkip
trait Skippable {
  case class ClassicEvent(eventType: String) extends EventSkip    // would be nice to have eventType as a enum-like hierarchy
  case class UnstructEvent(schema: SchemaKey) extends EventSkip

  def parse(json: JValue): ValidatedNelMessage[EventSkip] =
    json match {
      case JObject(fields) =>
        (fields.toMap.get("eventType"), fields.toMap.get("schema")) match {
          case (Some(JString("unstruct")), Some(JString(schemaUri))) =>
            SchemaKey.parse(schemaUri) match {
              case Success(a) => UnstructEvent(a).success
              case Failure(e) => e.failureNel
            }
          case (Some(JString(eventType)), None) =>
            ClassicEvent(eventType).success
          case _ =>
            "Object ${compact(json)} cannot be deserialized to EventSkip".toProcessingMessage.failureNel
        }
    }
}
