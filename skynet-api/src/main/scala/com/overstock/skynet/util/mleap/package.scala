package com.overstock.skynet.util

import com.overstock.skynet.domain.emptyRow
import com.overstock.skynet.util.render.{Op, OpOps}
import cats.{Eval, Id}
import cats.data.{Nested, NonEmptyVector}
import higherkindness.droste.data.Attr
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.types.{Casting, StructField, StructType}
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, LeapFrame, Row, Transformer}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}

import scala.util.{Success, Try}
import cats.implicits._
import cats.kernel.Semigroup
import zio.NonEmptyChunk

import java.net.URI

package object mleap {

  type OpTree = MleapOp[Transformer, AnyRef]

  implicit class FrameOps[FB <: LeapFrame[_]](private val fb: FB) extends AnyVal {

    def zip(other: LeapFrame[_]): Try[DefaultLeapFrame] =
      (fb.schema ++ other.schema)
        .map { schema =>
          val dataset = (fb.collect() zip other.collect()).map(((_: Row) concat (_: Row)).tupled)
          DefaultLeapFrame(schema, dataset)
        }


    def cross[Other <: LeapFrame[Other]](other: Other): Try[DefaultLeapFrame] = {
      val fSchema = fb.schema
      val oSchema = other.schema
      (fSchema ++ oSchema)
        .map { schema =>
          val crossedRows = mleap.crossRows(fSchema, fb.collect(), other.collect())
          DefaultLeapFrame(schema, crossedRows)
        }
    }

    def crossRows[Other <: LeapFrame[Other]](other: Other): Seq[Row] =
      mleap.crossRows(fb.schema, fb.collect(), other.collect())

    def crossRows(otherRows: Seq[Row]): Seq[Row] =
      mleap.crossRows(fb.schema, fb.collect(), otherRows)

    def prefix(colPrefix: String): Try[DefaultLeapFrame] =
      Try(DefaultLeapFrame(
        StructType(fb.schema.fields.map(f => f.copy(colPrefix + f.name))).get,
        fb.collect()
      ))
  }

  def crossRows(fSchema: StructType, fRows: Seq[Row], oRows: Seq[Row]): Seq[Row] = {
    for {
      otherRow  <- oRows
      fbRow     <- {
        val f = fRows
        if (f.isEmpty) NonEmptyChunk(emptyRow(fSchema)): Seq[Row] else f
      }
    } yield fbRow concat otherRow
  }

  implicit class StructOps(private val structType: StructType) extends AnyVal {

    def ++(other: StructType): Try[StructType] = StructType(structType.fields ++ other.fields)

    def diff(other: StructType): Try[StructType] = StructType(structType.fields diff other.fields)

    def forRows(rows: Seq[Row]): DefaultLeapFrame = DefaultLeapFrame(structType, rows)

    def fieldNamesId: String = structType.fields.map(_.name).toSet.mkString(" ")

    def crossRows(frameRows: Seq[Row], otherRows: Seq[Row]): Seq[Row] =
      mleap.crossRows(structType, frameRows, otherRows)
  }

  implicit class RowOps(private val row: Row) extends AnyVal {

    def concat(other: Row): Row = row.withValues(other.toSeq)

    def getRaw(field: String, structType: StructType): Any =
      structType
        .indexOf(field)
        .flatMap(idx => Try(row.getRaw(idx)))
  }

  implicit class ArrayRowOps(private val row: ArrayRow) extends AnyVal {

    def concat(other: Row): ArrayRow = row.withValues(other.toSeq)

    def getRaw(field: String, structType: StructType): Any =
      structType
        .indexOf(field)
        .flatMap(idx => Try(row.getRaw(idx)))
  }

  implicit val semigroupRow: Semigroup[Row] = _ concat _

  def createCastTransforms(inputSchema: StructType,
                           targetSchema: StructType): Try[Vector[(String, Any => Any)]] =
    inputSchema.fields
      .toVector
      .traverseFilter { case StructField(name, inputType) =>
        targetSchema
          .getField(name)
          .traverseFilter { case StructField(_, targetType) =>

            if (inputType != targetType) {

              Casting
                .cast(from = inputType, to = targetType)
                .nested.tupleLeft(name)
                .value.sequence

            } else {

              Success(None)
            }
          }
      }

  def getMLeapBundleVersion: String = Bundle.version

  def readModel(bundlePath: String): Try[Transformer] = {
    val bundle = BundleFile(new URI(bundlePath))
    val transformer = Try {
      bundle.loadMleapBundle().get.root
    }
    bundle.close()
    transformer
  }

  def readModelOp(bundlePath: String): Try[Op] = readModel(bundlePath).map(toModelOpTree(_)).map(_.value)

  type Drilldown[K, V] = Attr[({ type λ[α] = Map[K, α] })#λ, V]
  object Drilldown {

    def root[K]: PartiallyAppliedDrilldown[K] = new PartiallyAppliedDrilldown[K]

    class PartiallyAppliedDrilldown[K] private[Drilldown] {
      def apply[V](v: V): Drilldown[K, V] = Attr(v, Map.empty[K, Drilldown[K, V]])
    }
  }

  private def foldIndependant(independent: Stream[Transformer]): Eval[Option[Op]] = {
    independent
      .traverse(toModelOpTree(_, root = false))
      .flatMap {
        case Stream()             => Eval.now(None)
        case Stream(op)           => Eval.now(Some(op))
        case Stream(op, more @_*) => Eval.later(Some(Op.Par(NonEmptyVector.of(op, more: _*), None)))
      }
  }

  private def foldSequence(t: Stream[Transformer]): Eval[Option[Op]] = {

    if (t.isEmpty) Eval.now(None) else {

      val outNamesSet = t.foldr(Eval.now(Set.empty[String]))(
        (t, s) => (Eval.later(t.outputSchema.fields.map(_.name).toSet), s).mapN(_ ++ _))

      outNamesSet.flatMap { outNamesSet =>

        val (independent, dependent) = t
          .partition(_.inputSchema.fields.forall(f => !outNamesSet.contains(f.name)))

        for {
          parI    <- foldIndependant(independent)
          foldD   <- foldSequence(dependent)
        } yield (parI, foldD).mapN(_ andThen _) orElse foldD orElse parI
      }
    }
  }

  def toModelOpTree(transformer: Transformer, root: Boolean = true)
                   (implicit mleapContext: MleapContext): Eval[Op] = {

    import render._

    val children = transformer match {
      case Pipeline(_, _, PipelineModel(transformers)) => foldSequence(transformers.toStream)
      case _ => Eval.now(None)
    }


    val desc = children.flatMap { children =>
      Eval.later {
        val op = mleapContext.registry.opForObj[MleapContext, Transformer, AnyRef](transformer)
        Op.Description(
          name          = op.klazz.getSimpleName,
          opName        = op.name(transformer),
          inputSchema   = transformer.inputSchema,
          outputSchema  = transformer.outputSchema,
          children      = children)
      }
    }

    if (root) desc.map(Op.Root(_)) else desc
  }
}
