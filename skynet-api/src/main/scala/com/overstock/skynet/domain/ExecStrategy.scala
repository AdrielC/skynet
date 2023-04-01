
package com.overstock.skynet.domain

import cats.implicits._
import pureconfig.ConfigReader
import pureconfig.error._
import sttp.tapir.Codec.PlainCodec
import sttp.tapir.DecodeResult._
import sttp.tapir.EndpointIO.Example
import sttp.tapir.Schema.SName
import sttp.tapir.{CodecFormat, DecodeResult, Schema, ValidationError, Validator}
import zio.ExecutionStrategy

import scala.util.Try

sealed trait ExecStrategy extends Product with Serializable {
  override def toString: String = this match {
    case ExecStrategy.Sequential    => "seq"
    case ExecStrategy.Parallel      => "par"
    case ExecStrategy.ParallelN(n)  => s"par-$n"
  }
}
object ExecStrategy {

  final case object Sequential  extends ExecStrategy
  case object Parallel          extends ExecStrategy
  case class ParallelN(n: Int)  extends ExecStrategy

  implicit val schemaParallelN: Schema[ParallelN] = Schema
    .string
    .name(SName("ParallelN"))
    .description("Transform rows with parallelism up to N")
    .encodedExample(ParallelN(4).toString)
    .validate(Validator.min(1).contramap(_.n))

  implicit val schemaParallel: Schema[Parallel.type] = Schema
    .string
    .name(SName("Parallel"))
    .description("Transform rows with max parallelism")
    .encodedExample(Parallel.toString)

  implicit val schemaSequential: Schema[Sequential.type] = Schema
    .string
    .name(SName("Sequential"))
    .description("Transform rows sequentially")
    .encodedExample(Sequential.toString)

  private val desc = "Must be one of ['par', 'seq', 'par-n']"

  def parse(string: String): Option[ExecStrategy] = string match {
    case "par" => Some(Parallel)
    case "seq" => Some(Sequential)
    case other => other.split("-").toList match {
      case "par" :: n :: Nil => Try(n.toInt).toOption.map(ParallelN)
      case _ => None
    }
  }

  implicit val schemaExecutionStrategy: Schema[ExecStrategy] = Schema
    .string[ExecStrategy]
    .description("Specifies what parallelism level to use when transforming rows")

  def parseExecStrategy(str: String): Either[FailureReason, ExecStrategy] =
    plainCodecExecStrategy.decode(str) match {
      case Value(v) => v.asRight
      case other => CannotConvert(str, "ExecStrategy", desc + s". Found $other").asLeft
    }

  implicit val configReader: ConfigReader[ExecStrategy] = ConfigReader.fromString(parseExecStrategy)

  implicit val plainCodecExecStrategy: PlainCodec[ExecStrategy] = new PlainCodec[ExecStrategy] {

    override def rawDecode(l: String): DecodeResult[ExecStrategy] =
      ExecStrategy
        .parse(l)
        .map(Value(_))
        .getOrElse(InvalidValue(List(
          ValidationError.Custom(
            invalidValue = l,
            message = desc,
            path = Nil
          ))))

    override def encode(h: ExecStrategy): String = h.toString

    val schema: Schema[ExecStrategy] = schemaExecutionStrategy

    override val format: CodecFormat.TextPlain = CodecFormat.TextPlain()
  }

  val examples: List[Example[ExecStrategy]] = List(
    Example.of[ExecStrategy](ExecStrategy.ParallelN(4),
      summary = Some("Parallelism capped at 4")),
    Example.of(ExecStrategy.Parallel,
      summary = Some("Unbounded parallelism")),
    Example.of(ExecStrategy.Sequential,
      summary = Some("No parallelism")))


  implicit def toZIO(exec: ExecStrategy): ExecutionStrategy = exec match {
    case ExecStrategy.Sequential    => ExecutionStrategy.Sequential
    case ExecStrategy.Parallel      => ExecutionStrategy.Parallel
    case ExecStrategy.ParallelN(n)  => ExecutionStrategy.ParallelN(n)
  }
}