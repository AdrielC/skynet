package ml.combust.mleap.json.circe

import io.circe.Decoder.Result
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.Row
import ml.combust.mleap.tensor.{ByteString, Tensor}
import exd.fundamenski.skynet.domain._

import scala.util.Try

object RowCodec {

  case class MaybeNullableCodec(dataType: DataType)(implicit baseCodec: Codec[Any]) extends Codec[Any] {
    private val codec = if (dataType.isNullable) {
      basicCodec[Option[Any]].asInstanceOf[Codec[Any]]
    } else {
      baseCodec
    }
    override def apply(a: Any): Json = codec(a)
    override def apply(c: HCursor): Result[Any] = codec(c)
    def apply(row: Row, idx: Int): Try[Json] = Try {
      if (dataType.isNullable) {
        codec(Option(row.getRaw(idx)))
      } else {
        codec(row.getRaw(idx))
      }
    }
  }
  object MaybeNullableCodec {
    def apply[T: Encoder: Decoder](implicit dataType: DataType): MaybeNullableCodec =
      MaybeNullableCodec(dataType)(basicCodec[T].asInstanceOf[Codec[Any]])
  }

  private def listSerializer(lt: ListType): MaybeNullableCodec = {
    implicit val dt = lt
    implicit val s: Codec[Any] = basicSerializer(lt.base, isNullable = false)
    MaybeNullableCodec[Seq[Any]]
  }

  private def tensorSerializer(tt: TensorType): MaybeNullableCodec = {
    implicit val isNullable = tt
    implicit val missingTensorDims: Impute.Dims = Impute.Dims(tt.dimensions)
    implicit val s: Codec[Any] = basicSerializer(tt.base, isNullable = false)
    MaybeNullableCodec[Tensor[Any]]
  }

  private def basicSerializer(base: BasicType, isNullable: Boolean): MaybeNullableCodec = {
    implicit val dt = ScalarType(base, isNullable)
    base match {
      case BasicType.Boolean    => MaybeNullableCodec[Boolean]
      case BasicType.Byte       => MaybeNullableCodec[Byte]
      case BasicType.Short      => MaybeNullableCodec[Short]
      case BasicType.Int        => MaybeNullableCodec[Int]
      case BasicType.Long       => MaybeNullableCodec[Long]
      case BasicType.Float      => MaybeNullableCodec[Float]
      case BasicType.Double     => MaybeNullableCodec[Double]
      case BasicType.String     => MaybeNullableCodec[String]
      case BasicType.ByteString => MaybeNullableCodec[ByteString]
    }
  }

  private def scalarSerializer(st: ScalarType): MaybeNullableCodec =
    basicSerializer(st.base, st.isNullable)

  def serializer(dt: DataType): MaybeNullableCodec = dt match {
    case st: ScalarType   => scalarSerializer(st)
    case lt: ListType     => listSerializer(lt)
    case tt: TensorType   => tensorSerializer(tt)
  }

  private def basicCodec[A](implicit E: Encoder[A], D: Decoder[A]): Codec[A] =
    Codec.from(D, E)
}

case class RowCodec(schema: StructType, imputeNulls: Boolean = true) extends Codec[Row] {

  import cats.implicits._

  private val imputer: DataType => Any => Any = {
    val imp = (e: Any) => (_: Any).asInstanceOf[Option[Any]].getOrElse(e)
    if (imputeNulls) {
      dt => if (dt.isNullable) imp(imputeDataType(dt)) else identity
    } else {
      dt => if (dt.isNullable) imp(null) else identity
    }
  }

  private lazy val serializers = schema
    .fields
    .toList
    .mapWithIndex((struct, idx) => (RowCodec.serializer(struct.dataType), idx))

  override def apply(obj: Row): Json =
    Json.fromValues(serializers.map { case (s, i) => s(obj, i).get })

  override def apply(c: HCursor): Result[Row] =
    for {
      arr         <- c.values.toRight(DecodingFailure("invalid row", c.history))
      elements    = arr.toVector
      rowValues   <- Try(serializers
        .map { case (s, i) => imputer(s.dataType)(s.decodeJson(elements(i)).toTry.get) })
        .toEither
        .left
        .map(DecodingFailure.fromThrowable(_, c.history))

    } yield Row(rowValues: _*)
}