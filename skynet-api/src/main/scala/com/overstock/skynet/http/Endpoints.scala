package com.overstock.skynet.http

import com.overstock.skynet.{BuildInfo, http}
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import sttp.tapir._
import com.overstock.skynet.util.json._
import ml.combust.mleap.json.circe._
import sttp.tapir.openapi.circe.yaml._
import cats.implicits._
import com.overstock.skynet.domain.Req.{RankFrame, TransformFrame}
import com.overstock.skynet.domain.{ExecStrategy, Frame, GetSample, GraphModel, HandleMissing, Model, ModelInfo, RankingResult, Select}
import sttp.tapir.EndpointIO.Example
import sttp.tapir.docs.openapi.OpenAPIDocsInterpreter

import java.net.URI

import scala.xml.NodeSeq

class Endpoints(models: Map[String, URI] = Map.empty) {

  private val modelsPath = "models"

  private val modelPath =
    modelsPath / path[String]("model")
      .description("Name of the model")
      .examples(models.keys.map(Example.of(_)).toList)

  private val selectQuery =
    query[List[Select]]("select")
      .schema(Select.schemaSelect.asIterable[List])
      .default(Nil)
      .examples(List(
        Example.of(Nil,
          name = Some("No selection"),
          summary = Some("Return all columns")),
        Example.of(
          List(Select.field("result_prod_id")),
          summary = Some("Select single column")),
        Example.of(List(
          Select.field("result_prod_id"),
          Select.field("prediction"),
          Select.many("context_prod_ids,context_prod_id")),
          summary = Some("Select multiple columns"))))
      .description("Selects the following fields from the result in order of input. " +
        "Also accepts a comma seperated list of fields")
      .map(_.toNel.map(_.reduce))(_.toList)

  private val selEx: List[Example[Select]] = List(
    Example.of(
      Select.field("prediction"),
      summary = Some("Rank by the 'prediction' field")),
    Example.of(
      Select.field("prediction*result_list_price"),
      summary = Some("Rank by the product of the 'prediction' and 'result_list_price' fields")))

  val rankQuery = query[Option[Select]]("rank")
    .description("After transforming the rows, rank by the field selection if provided")
    .examples(selEx.map(_.map(_.some)))

  val rankReqQuery = query[Select]("rank")
    .description("After transforming the rows, rank by the field selection")
    .examples(selEx)

  private val nRowQuery = query[Int]("nrow")
    .description("The number of randomly generated rows to include in the sample LeapFrame")
    .default(1)

  private val handleMissing = query[HandleMissing]("missing")
    .description("optional strategy to use when handling cases where the input " +
      "Frame is missing a column required by the target transformer")
    .default(HandleMissing.Impute())
    .examples(List(
      Example.of(
        HandleMissing.Impute(),
        name = Some("Impute"),
        summary = Some("Imputes any missing required fields to the empty value based on the field's type")),
      Example.of(
        HandleMissing.Error(),
        name = Some("Error"),
        summary = Some("Error out if any required fields are missing"))))

  private val execQuery = query[Option[ExecStrategy]]("exec")
    .schema(ExecStrategy.schemaExecutionStrategy.asOption)
    .description("Parallelism/execution-strategy to use for this transformation")
    .examples(ExecStrategy.examples.map(_.map(_.some)))

  private def frameBody = jsonBody[Frame](Frame.codecFrame, Frame.codecFrame, Frame.frameSchema)

  private val frameIn = frameBody
    .description("Frame of rows to transform")
    .examples(Frame.sample.examples)

  private val sampleFrameOut = frameBody
    .description("Sample Frame with schema expected by this model")
    .examples(Frame.sample.sampleExamples)

  val transformEndpoint =
    endpoint
      .name("transform")
      .post
      .description("Transforms a Leapframe with the given model")
      .in(modelPath / "transform")
      .in(query[Option[Int]]("k")
        .description("If ranking, returns the top k rows")
        .examples(List(
          Example.of(Some(3), summary = Some("Return only the top 3")),
          Example.of(None, summary = Some("Return all"))
        )))
      .in(selectQuery)
      .in(rankQuery)
      .in(query[Boolean]("desc")
        .description("If ranking, ranks by the given rank param in descending if true and vice versa")
        .default(true))
      .in(execQuery)
      .in(handleMissing)
      .in(frameIn)
      .out(jsonBody[DefaultLeapFrame]
        .description("A DefaultLeapFrame that contains all of the columns added by the transformation, " +
          "and those originally present in the input"))
      .errorOut(jsonBody[http.ErrorResponse])
      .mapInTo[TransformFrame]

  val rankEndpoint =
    endpoint
      .name("rank")
      .post
      .description("""
        |First transforms the input frame, then ranks the model output by the specified 'rank' column,
        |optionally ranking by the average rank over id using the 'avg' query param,
        |and/or ranking withing a specified group using the 'group' param and then ranking the entire,
        |unique-by-group result set
        |""".stripMargin)
      .in(modelPath / "rank")
      .in(query[Option[Int]]("k")
        .description("Returns the top k ranked rows. Default returns all")
        .example(Some(3)))
      .in(query[String]("id")
        .description("Selects this single column from the ranked result")
        .example("result_prod_id"))
      .in(rankReqQuery)
      .in(query[Boolean]("desc")
        .description("Rank descending by rank if true, ascending if false")
        .default(true))
      .in(query[Option[String]]("group")
        .description("""
          |Takes the row with the max 'rank' within each unique 'group' value.
          |
          |The 'group' logic is equivalent to the following SQL:
          |
          |```
          |SELECT <id>
          |FROM (
          |   SELECT <id>,
          |          <rank>,
          |          ROW_NUMBER() OVER(PARTITION BY <group> ORDER BY <rank> DESC) group_rank
          |   FROM <model>
          |)
          |WHERE group_rank = 1
          |ORDER BY <rank> DESC
          |LIMIT <k>
          |```
          |""".stripMargin)
        .examples(List(
          Example.of(
            Some("result_prod_id"),
            summary = Some("Group by field")),
          Example.of(
            None,
            name = Some("No grouping")),
          Example.of(
            Some("parent_item_id"),
            summary = Some("Group by a field not in required input schema")))))
      .in(query[Boolean]("avg")
        .description("""
          |Whether to average the ranks by 'id'
          |
          |The 'avg' logic is equivalent to the following SQL:
          |
          |```
          |SELECT <id>
          |FROM (
          |   SELECT <id>, AVG(<rank>)
          |   FROM <model>
          |   GROUP BY <id>
          |)
          |ORDER BY AVG(<rank>) DESC
          |LIMIT <k>
          |```
          |""".stripMargin)
        .default(false))
      .in(execQuery)
      .in(handleMissing)
      .in(frameIn)
      .out(jsonBody[RankingResult])
      .errorOut(jsonBody[http.ErrorResponse])
      .mapInTo[RankFrame]

  val modelHealthCheck  =
    endpoint
      .name("model-health")
      .get
      .description("Healthcheck for a loaded model. Runs a test prediction " +
        "against the model with auto-generated sample data")
      .in(modelPath / "health")
      .in(query[Int]("n")
        .description("The number of randomly generated LeapFrames to test on the model")
        .default(1))
      .in(nRowQuery)
      .errorOut(jsonBody[http.ErrorResponse])
      .name("model-health")

  val serviceHealthCheck  =
    endpoint
      .name("service-health")
      .get
      .description("Healthcheck for the service")
      .in("health")
      .errorOut(jsonBody[http.ErrorResponse])

  val modelVisualize =
    endpoint
      .name("graph")
      .get
      .description("Visualize the internal computation graph of a loaded model")
      .in(modelPath / "graph")
      .in(query[Boolean]("elide-parent-schema").default(true))
      .in(query[Boolean]("unique-schemas").default(true))
      .in(query[Int]("density").default(50))
      .in(query[Double]("vertical-spacing").default(0.5))
      .out(xmlBody[NodeSeq])
      .errorOut(jsonBody[http.ErrorResponse])
      .mapInTo[GraphModel]

  val getSampleInput =
    endpoint
      .name("sample")
      .get
      .description("Generates a sample DefaultLeapFrame with the expected " +
        "schema for the particular model hosted at this path")
      .in(modelPath / "sample")
      .in(nRowQuery)
      .in(query[List[String]]("context")
        .description("A list of context prefixes to construct a sample CartesianFrame with. " +
          "If supplied, will return a cartesian frame with a sub-frame for each provided context in order," +
          " given there exist field names that start with any of the provided context prefixes. " +
          "This param accepts a comma seperated list to indicate multiple contexts, " +
          "and it also accepts a url encoded '|' (encoding is '%7C') " +
          "as an OR matching operator to combine multiple prefixes in a single context")
        .example(List("user|context_prod_ids", "context"))
        .default(Nil)
        .map(_.flatMap(_.split(',')))(identity))
      .in(selectQuery)
      .out(sampleFrameOut)
      .errorOut(jsonBody[http.ErrorResponse])
      .mapInTo[GetSample]

  val loadModelEndpoint =
    endpoint
      .name("load")
      .put
      .description(
        "Loads a model from the given URI " +
          "After it has been downloaded and deserialized, the loaded model can be accessed at " +
          "the same path as this request")
      .in(modelPath)
      .in(plainBody[URI]
        .description("Uri of the model to load. Valid URIs are " +
          "['file://...', 's3://...', 'gs://<bucket>/<obj>...']"))
      .out(jsonBody[Model])
      .errorOut(jsonBody[http.ErrorResponse])
      .mapInTo[Model]

  val unloadModelEndpoint =
    endpoint
      .name("unload")
      .delete
      .description("Unloads a model from this service")
      .in(modelPath)
      .out(jsonBody[Option[ModelInfo]]
        .description("If found, returns the model info of the evicted model"))
      .errorOut(jsonBody[http.ErrorResponse])

  val getModelEndpoint =
    endpoint
      .name("describe")
      .get
      .description("Retrieves information including origin uri, input/output schemas, " +
        "and metadata of a the specified loaded model")
      .in(modelPath)
      .out(jsonBody[ModelInfo])
      .errorOut(jsonBody[http.ErrorResponse])

  val getModelsEndpoint =
    endpoint
      .name("list")
      .get
      .description("Returns a list of all model names currently loaded in memory by this service")
      .in(modelsPath)
      .out(jsonBody[List[Model]])
      .errorOut(jsonBody[http.ErrorResponse])

  private[http] val utilityEndpoints = List(
    getModelEndpoint,
    getModelsEndpoint,
    getSampleInput,
    modelVisualize,
    modelHealthCheck,
    serviceHealthCheck
  )

  private[http] val documentedEndpoints =
    loadModelEndpoint +:
    unloadModelEndpoint +:
    rankEndpoint +:
    transformEndpoint +:
    utilityEndpoints

  val docsYaml: String = OpenAPIDocsInterpreter()
    .toOpenAPI(
      es      = documentedEndpoints,
      title   = "Skynet API",
      version = BuildInfo.version)
    .toYaml
}

