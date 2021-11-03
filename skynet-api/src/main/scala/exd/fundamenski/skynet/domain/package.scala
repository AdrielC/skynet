package exd.fundamenski.skynet

import exd.fundamenski.skynet.domain.Frame.LeapFrame
import ml.combust.mleap.core.types.{BasicType, DataType, ListType, ScalarType, StructType, TensorType}
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.{ByteString, DenseTensor, Tensor}
import zio.{Chunk, NonEmptyChunk, Task}
import java.util.Base64

import exd.fundamenski.skynet.domain.Frame.LeapFrame

import scala.util.{Random, Try}

package object domain {

  implicit class RichTry[T](f: Try[T]) {
    def task: Task[T] = Task.fromTry(f)
  }

  def createSampleLeapFrame(schema: StructType, n: Int = 1): LeapFrame =
    LeapFrame(DefaultLeapFrame(
      schema = schema,
      dataset = Seq.fill(n)(Row(schema.fields.map(f => getRandomValue(f.dataType)): _*))))

  def getRandomBasicValue(base: BasicType): Any = {
    def randomString = List.fill(Random.nextInt() % 10)(Random.nextPrintableChar()).mkString("")
    base match {
      case BasicType.Boolean    => Random.nextBoolean()
      case BasicType.Byte       => Random.nextInt().##.byteValue()
      case BasicType.Short      => (Random.nextInt() % 100L).toShort
      case BasicType.Int        => Random.nextInt() % 1000
      case BasicType.Long       => Random.nextLong() % 1000L
      case BasicType.Float      => Random.nextFloat()
      case BasicType.Double     => Random.nextDouble()
      case BasicType.String     => randomString
      case BasicType.ByteString => Base64.getEncoder.encodeToString(ByteString(randomString.getBytes).bytes)
    }
  }

  def getRandomValue(d: DataType): Any =
    d match {
      case ScalarType(base, _)                    => getRandomBasicValue(base)
      case ListType(base, _)                      => List.fill(Random.nextInt().abs % 10)(getRandomBasicValue(base))
      case TensorType(_, None, _)                 => DenseTensor(Array(), Seq())
      case TensorType(base, Some(dimensions), _)  =>
        val values = dimensions.flatMap(List.fill(_)(getRandomBasicValue(base))).toArray
        Tensor.create(values, dimensions, None)
    }

  def imputeBasicType(d: BasicType): Any = d match {
    case BasicType.Boolean    => Impute.impute[Boolean]
    case BasicType.Byte       => Impute.impute[Byte]
    case BasicType.Short      => Impute.impute[Short]
    case BasicType.Int        => Impute.impute[Int]
    case BasicType.Long       => Impute.impute[Long]
    case BasicType.Float      => Impute.impute[Float]
    case BasicType.Double     => Impute.impute[Double]
    case BasicType.String     => Impute.impute[String]
    case BasicType.ByteString => Impute.impute[ByteString]
  }

  def imputeDataType(d: DataType): Any = d match {
    case ScalarType(base, _)          => imputeBasicType(base)
    case TensorType(_, dimensions, _) => implicit val dims = Impute.Dims(dimensions); Impute.imputeTensor
    case ListType(_, _)               => Impute.impute[List[Any]]
  }

  def emptyRow(schema: StructType): Row =
    ArrayRow(schema.fields.map(d => imputeDataType(d.dataType)))

  def emptyFrame(schema: StructType): DefaultLeapFrame =
    DefaultLeapFrame(schema, NonEmptyChunk(emptyRow(schema)))
}
