package com.overstock.skynet.domain

import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, FrameBuilder, Row, LeapFrame => LFrame}
import cats.implicits._
import ml.combust.mleap.core.types.{BasicType, DataType, ListType, ScalarShape, ScalarType, StructType}
import sttp.tapir.Schema
import com.overstock.skynet.util.json._
import com.overstock.skynet.util.mleap._
import ml.combust.mleap.json.circe._

import scala.util.{Success, Try}
import Frame._
import io.circe.Decoder.Result
import io.circe.generic.semiauto
import io.circe.syntax.EncoderOps
import ml.combust.mleap.json.circe.RowCodec
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
import sttp.tapir.EndpointIO.Example
import sttp.tapir.Schema.SName

sealed trait Frame extends Product with Serializable with LFrame[Frame] {

  def flatten: Try[DefaultLeapFrame]

  override def withColumns(names: Seq[String], selectors: Selector*)(udf: UserDefinedFunction): Try[Frame] =
    flatten.flatMap(_.withColumns(names, selectors: _*)(udf).map(LeapFrame(_)))
  override def drop(names: String*): Try[Frame] =
    flatten.flatMap(_.drop(names: _*).map(LeapFrame(_)))
  override def select(fieldNames: String*): Try[Frame] =
    flatten.flatMap(_.select(fieldNames: _*).map(LeapFrame(_)))
  override def filter(selectors: Selector*)(udf: UserDefinedFunction): Try[Frame] =
    flatten.flatMap(_.filter(selectors: _*)(udf).map(LeapFrame(_)))
  override def withColumn(name: String, selectors: Selector*)(udf: UserDefinedFunction): Try[Frame] =
    flatten.flatMap(_.withColumn(name, selectors: _*)(udf).map(LeapFrame(_)))
  override def collect(): Seq[Row] = flatten.map(_.collect()).getOrElse(Nil)
}

object Frame {

  object sample {
    private val ctxSchema = StructType("cluster_products" -> ListType(BasicType.String)).get
    private val resSchemas = StructType(
      "ticker"           -> ScalarType(BasicType.String),
      "price"  -> ScalarType(BasicType.Double),
      "price_1day_high" -> ScalarType(BasicType.Double)).get

    val leapFrame: LeapFrame = LeapFrame((
      createSampleLeapFrame(ctxSchema, 2) zip
        createSampleLeapFrame(resSchemas, 2)).get)

    val cartesianFrame: CartesianFrame = CartesianFrame(
      contextFrame = createSampleLeapFrame(ctxSchema),
      targetFrame = createSampleLeapFrame(resSchemas, 2))

    val contextFrame: ContextFrame = {
      val contextSchema = ctxSchema
      val targetSchema = resSchemas
      ContextFrame(
        contextSchema = contextSchema,
        targetSchema = targetSchema,
        rows = Seq(
          ContextRows(
            contextRow = createSampleLeapFrame(contextSchema).frame.dataset.head,
            targetRows = createSampleLeapFrame(targetSchema, 2).frame.dataset),
          ContextRows(
            contextRow = createSampleLeapFrame(contextSchema).frame.dataset.head,
            targetRows = createSampleLeapFrame(targetSchema, 2).frame.dataset))
      )
    }

    val prefixedFrame: PrefixedFrame = PrefixedFrame(
      frame = createSampleLeapFrame(StructType(
        "ticker"           -> ScalarType(BasicType.String),
        "price"  -> ScalarType(BasicType.Double),
        "price_1day_high" -> ScalarType(BasicType.Double)).get),
      prefix = "result_")

    val sampleExamples: List[Example[Frame]] = List(
      Example.of(sample.leapFrame, name = Some("Leap Frame")),
      Example.of(sample.cartesianFrame, name = Some("Cartesian Frame")))

    val examples: List[Example[Frame]] =
      sampleExamples ++ List(
        Example.of(sample.prefixedFrame, name = Some("Prefixed Frame")),
        Example.of(sample.contextFrame, name = Some("Context Frame")))
  }


  case class ContextFrame(contextSchema: StructType,
                          targetSchema: StructType,
                          rows: Seq[ContextRows]) extends Frame {

    override lazy val flatten: Try[DefaultLeapFrame] =
      (contextSchema ++ targetSchema)
        .map(_.forRows(rows.flatMap(ctx => ctx.targetRows.map(ctx.contextRow concat _))))

    override def select(fieldNames: String*): Try[Frame] =
       contextSchema
         .select(fieldNames: _*)
         .map { s => s -> true }
         .orElse(targetSchema.select(fieldNames: _*).map(_ -> false))
         .map { case (s, isContext) =>
           if (isContext) {
             val idx = contextSchema.indicesOf(fieldNames: _*).get
             val r = rows.map(ctx =>
               ctx.copy(contextRow = ctx.contextRow.selectIndices(idx: _*)))
             ContextFrame(s, targetSchema, r)
           } else {
             val idx = targetSchema.indicesOf(fieldNames: _*).get
             val r = rows.map(ctx =>
               ctx.copy(targetRows = ctx.targetRows.map(_.selectIndices(idx: _*))))
             ContextFrame(contextSchema, s, r)
           }
         }

    override lazy val schema: StructType = (contextSchema ++ targetSchema).get
  }

  object ContextFrame {

    implicit val codecContextFrame: Codec[ContextFrame] = new Codec[ContextFrame] {

      override def apply(a: ContextFrame): Json = {
        val contextRowCodec = RowCodec(a.contextSchema)
        val targetRowCodec = RowCodec(a.targetSchema)

        Json.fromFields(Map(
          "contextSchema" -> a.contextSchema.asJson,
          "targetSchema" -> a.targetSchema.asJson,
          "rows" -> Json.fromValues(a.rows.map {
            case ContextRows(contextRow, targetRows) =>
              Json.fromFields(Map(
                "contextRow" -> contextRowCodec(contextRow),
                "targetRows" -> Json.fromValues(targetRows.map(targetRowCodec(_)))
              ))
          })
        ))
      }

      override def apply(c: HCursor): Result[ContextFrame] = {
        for {
          contextSchema <- c.get[StructType]("contextSchema")
          targetSchema <- c.get[StructType]("targetSchema")
          allSchema <- (contextSchema ++ targetSchema).toEither
            .leftMap(e => DecodingFailure(e.getLocalizedMessage, c.history))
          rows <- {
            implicit val contextRowDecoder: Decoder[Row] = RowCodec(contextSchema)
            implicit val targetRowDecoder: Decoder[Seq[Row]] = Decoder.decodeSeq(RowCodec(targetSchema))
            implicit val rowsDecoder: Decoder[ContextRows] =
              Decoder.forProduct2("contextRow", "targetRows")(ContextRows(_, _))

            c.get[Seq[ContextRows]]("rows")
          }
        } yield new ContextFrame(
          contextSchema = contextSchema,
          targetSchema = targetSchema,
          rows = rows
        ) {
          override lazy val schema: StructType = allSchema
        }
      }
    }


    implicit lazy val contextFrameSchema: Schema[ContextFrame] = Schema
      .derived
      .encodedExample(sample.contextFrame.asJson.spaces4)
  }

  case class ContextRows(contextRow: Row, targetRows: Seq[Row])
  object ContextRows {

    implicit lazy val contextRowsSchema: Schema[ContextRows] = Schema.derived
  }

  case class PrefixedFrame(frame: Frame, prefix: String) extends Frame {

    override lazy val flatten: Try[DefaultLeapFrame] = frame.prefix(prefix)

    override def select(fieldNames: String*): Try[Frame] = fieldNames
      .toList
      .traverse(Success(_).filter(_.startsWith(prefix)).map(_.take(prefix.length)))
      .flatMap(frame.select(_: _*))

    override lazy val schema: StructType = {
      val innerSchema = frame.schema
      innerSchema.copy(fields = innerSchema.fields.map(f => f.copy(prefix + f.name)))
    }
  }
  object PrefixedFrame {

    implicit lazy val prefixedFrameCodec: Codec[PrefixedFrame] = semiauto.deriveCodec

    implicit lazy val contextFrameSchema: Schema[PrefixedFrame] = Schema
      .derived
      .description("Wraps a single Frame alongside a prefix that will be " +
        "prepended to each field name in the enclosed frames schema")
      .encodedExample(sample.prefixedFrame.asJson.spaces4)
  }

  case class CartesianFrame(contextFrame: Frame, targetFrame: Frame) extends Frame {

    override lazy val flatten: Try[DefaultLeapFrame] = contextFrame cross targetFrame

    private lazy val rows = contextFrame crossRows targetFrame

    override lazy val schema: StructType = (contextFrame.schema ++ targetFrame.schema).get

    override def collect(): Seq[Row] = rows

    override def select(fieldNames: String*): Try[Frame] =
      contextFrame.select(fieldNames: _*) orElse targetFrame.select(fieldNames: _*)
  }
  object CartesianFrame {

    implicit lazy val codecCartesianFrame: Codec[CartesianFrame] = semiauto.deriveCodec[CartesianFrame]
      .iemap(e => Try(e.schema).as(e).toEither.leftMap(_.getMessage))(identity)

    implicit lazy val cartesianFrameSchema: Schema[CartesianFrame] = Schema
      .derived
      .encodedExample(sample.cartesianFrame.asJson.spaces4)
  }

  case class LeapFrame(frame: DefaultLeapFrame) extends Frame {

    override def flatten: Try[DefaultLeapFrame] = Success(frame)

    override def withColumns(names: Seq[String], selectors: Selector*)(udf: UserDefinedFunction): Try[Frame] =
      frame.withColumns(names, selectors: _*)(udf).map(LeapFrame(_))
    override def drop(names: String*): Try[Frame] =
      frame.drop(names: _*).map(LeapFrame(_))
    override def select(fieldNames: String*): Try[Frame] =
      frame.select(fieldNames: _*).map(LeapFrame(_))
    override def filter(selectors: Selector*)(udf: UserDefinedFunction): Try[Frame] =
      frame.filter(selectors: _*)(udf).map(LeapFrame(_))
    override def withColumn(name: String, selectors: Selector*)(udf: UserDefinedFunction): Try[Frame] =
      frame.withColumn(name, selectors: _*)(udf).map(LeapFrame(_))
    override val schema: StructType = frame.schema
  }
  object LeapFrame {

    def apply(schema: StructType,
              dataset: Seq[Row]): LeapFrame = LeapFrame(DefaultLeapFrame(schema, dataset))

    def apply(schema: (String, ml.combust.mleap.core.types.BasicType)*)
             (dataset: Row*): Try[LeapFrame] =
      StructType(schema.toSeq.map(a =>
        ml.combust.mleap.core.types.StructField(
          a._1 -> DataType(a._2, ScalarShape(true)))))
        .map(s => LeapFrame(DefaultLeapFrame(s, dataset)))

    implicit val leapFrameCodec: Codec[LeapFrame] =
      MleapDefaultLeapFrameReaderCodec.imap(LeapFrame(_))(_.frame)

    implicit val leapFrameSchema: Schema[LeapFrame] =
      schemaDefaultLeapFrame
        .map(LeapFrame(_).some)(_.frame)
        .name(SName("LeapFrame"))
        .encodedExample(sample.leapFrame.asJson.spaces4)
  }

  implicit lazy val codecFrame: Codec[Frame] = Codec.from(
      (LeapFrame.leapFrameCodec: Decoder[LeapFrame]).widen[Frame] or
      (CartesianFrame.codecCartesianFrame: Decoder[CartesianFrame]).widen[Frame] or
      (ContextFrame.codecContextFrame: Decoder[ContextFrame]).widen[Frame] or
      (PrefixedFrame.prefixedFrameCodec: Decoder[PrefixedFrame]).widen[Frame],
    Encoder.instance {
      case l: LeapFrame       => LeapFrame.leapFrameCodec(l)
      case c: CartesianFrame  => CartesianFrame.codecCartesianFrame(c)
      case c: ContextFrame    => ContextFrame.codecContextFrame(c)
      case p: PrefixedFrame   => PrefixedFrame.prefixedFrameCodec(p)
    })

  implicit lazy val frameSchema: Schema[Frame] =
    Schema
      .derived
      .name(SName("Frame"))
      .description("Sealed trait hierarchy of Frame subtypes. " +
        "Each Frame subtype that will be flattened to LeapFrame prior to transformation")
}


