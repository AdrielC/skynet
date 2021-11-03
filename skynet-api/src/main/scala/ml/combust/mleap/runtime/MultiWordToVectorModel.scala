package ml.combust.mleap.runtime

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Value, Model => BundleModel}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.feature.WordToVectorKernel
import ml.combust.mleap.core.types.{BasicType, ListType, NodeShape, Socket, StructField, StructType, TensorType}
import ml.combust.mleap.runtime.frame.{FrameBuilder, MultiTransformer, Transformer}
import ml.combust.mleap.runtime.function.{FieldSelector, UserDefinedFunction}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.util.Try

case class MultiWordToVectorModel(wordIndex: Map[String, Int],
                                  wordVectors: Array[Double],
                                  kernel: WordToVectorKernel = WordToVectorKernel.Default) extends Model {
  val numWords: Int = wordIndex.size
  val vectorSize: Int = wordVectors.length / numWords
  @transient lazy val vectors: Map[String, Vector] = {
    wordIndex.map { case (word, ind) =>
      (word, wordVectors.slice(vectorSize * ind, vectorSize * ind + vectorSize))
    }
  }.mapValues(Vectors.dense).map(identity)

  def apply(sentence: Seq[String]): Vector = {
    if (sentence.isEmpty) {
      Vectors.sparse(vectorSize, Array.empty[Int], Array.empty[Double])
    } else {
      val vs = sentence.iterator.map(vectors.get).
        filter(_.isDefined).
        map(_.get)
      kernel(vectorSize, sentence.size, vs)
    }
  }

  val inputSchema: StructType = StructType("input" -> ListType(BasicType.String)).get

  val outputSchema: StructType = StructType("output" -> TensorType.Double(vectorSize)).get
}


/**
 * Created by hollinwilkins on 12/28/16.
 */
case class MultiWordToVector(override val uid: String = Transformer.uniqueName("multi_word_to_vector"),
                             override val shape: NodeShape,
                             @transient override val model: MultiWordToVectorModel) extends Transformer {

  override lazy val inputSchema: StructType =
    StructType(shape.inputs
      .toSeq
      .map { case (_, Socket(_, name)) => StructField(name, ListType(BasicType.String)) })
      .get

  override lazy val outputSchema: StructType =
    StructType(shape.outputs
      .map { case (_, Socket(_, name)) => StructField(name, TensorType.Double(model.vectorSize)) }
      .toSeq)
      .get

  private val insOuts = (inputSchema.fields.map(_.name) zip outputSchema.fields.map(_.name)).toList

  import ml.combust.mleap.core.util.VectorConverters._
  import cats.implicits._

  override def transform[FB <: FrameBuilder[FB]](builder: FB): Try[FB] =
    insOuts.foldLeftM(builder)((b, c) => b.withColumn(c._2, FieldSelector(c._1))(
      (sentence: Seq[String]) => model(sentence): Tensor[Double]))
}


class MultiWordToVectorOp extends MleapOp[MultiWordToVector, MultiWordToVectorModel] {
  override val Model: OpModel[MleapContext, MultiWordToVectorModel] =
    new OpModel[MleapContext, MultiWordToVectorModel] {
    override val klazz: Class[MultiWordToVectorModel] = classOf[MultiWordToVectorModel]

    override def opName: String = "multi_word_to_vector"

    override def store(model: BundleModel, obj: MultiWordToVectorModel)
                      (implicit context: BundleContext[MleapContext]): BundleModel = {
      val (words, indices) = obj.wordIndex.toSeq.unzip
      model.withValue("words", Value.stringList(words)).
        withValue("indices", Value.longList(indices.map(_.toLong))).
        withValue("word_vectors", Value.doubleList(obj.wordVectors)).
        withValue("kernel", Value.string(obj.kernel.name))
    }

    override def load(model: BundleModel)
                     (implicit context: BundleContext[MleapContext]): MultiWordToVectorModel = {
      val words = model.value("words").getStringList

      // If indices list is not set explicitly, assume words are ordered 0 to n
      val indices = model.getValue("indices").
        map(_.getLongList.map(_.toInt)).
        getOrElse(words.indices)
      val map = words.zip(indices).toMap
      val wv = model.value("word_vectors")
      val wordVectors = Try(wv.getDoubleList.toArray).orElse(Try(wv.getTensor[Double].rawValues)).get
      val kernel = model.getValue("kernel").
        map(_.getString).
        map(WordToVectorKernel.forName).
        getOrElse(WordToVectorKernel.Default)

      MultiWordToVectorModel(wordIndex = map,
        wordVectors = wordVectors,
        kernel = kernel)
    }
  }

  override def model(node: MultiWordToVector): MultiWordToVectorModel = node.model
}

