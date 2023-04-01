package com.overstock.skynet.service

import com.overstock.skynet.domain.{ExecStrategy, Frame, HandleMissing, emptyFrame, emptyRow}
import com.overstock.skynet.http.TryWrapper
import com.overstock.skynet.util.mleap._
import com.overstock.skynet.util.render.Op
import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging
import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.executor.TransformError
import ml.combust.mleap.runtime.{ChunkedRow, LeapFrameLike}
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, Row, RowTransformer, Transformer}
import zio.blocking.{Blocking, blocking}
import zio.{Chunk, Has, NonEmptyChunk, RIO, Task, ZIO}

import scala.util.Try

package object transform {

  type Transform = Has[Transform.Service]
  object Transform extends Serializable {

    trait Service extends Serializable {

      def transform[F: LeapFrameLike]
      (dataset: F,
       exec: ExecStrategy,
       select: Option[NonEmptyList[String]] = None,
       handleMissing: HandleMissing = HandleMissing.Error()): RIO[Blocking, DefaultLeapFrame]

      def inputSchema: StructType

      def outputSchema: StructType

      def op: Op
    }

    object Service {

      class InMemoryTransformer private[Service] (
       rowTransformer: RowTransformer,
       val op: Op
      ) extends Transform.Service with LazyLogging {

        val inputSchema: StructType = rowTransformer.inputSchema
        val outputSchema: StructType = rowTransformer.outputSchema

        private val inFields = inputSchema.fields.map(_.name)
        private val outFields = outputSchema.fields.map(_.name)

        private val debugMissingFieldLimit = 5

        def transform[F: LeapFrameLike](dataset: F,
                      exec: ExecStrategy,
                      select: Option[NonEmptyList[String]] = None,
                      handleMissing: HandleMissing = HandleMissing.Error()): RIO[Blocking, DefaultLeapFrame] =
          for {
            (crossed, selectedSchema, transform) <- blocking(for {
              c               <- preprocessFrame(dataset, handleMissing)
              (ss, t)         <- createTransform(c.schema, select).runTask
            } yield (c, ss, t))
            transformedData   <- blocking(ZIO.foreachExec(crossed.dataset)(exec)(transform))
          } yield DefaultLeapFrame(selectedSchema, transformedData)

        private def preprocessFrame[F](frame: F, handleMissing: HandleMissing)
                                      (implicit L: LeapFrameLike[F]): Task[DefaultLeapFrame] = Task {

          lazy val frameSchema = L.schema(frame)

          val missingFields = Chunk.fromIterable(inputSchema.fields)
            .filterNot(field => frameSchema.fields.exists(_.name == field.name))

          val nMissing = missingFields.length

          if (nMissing > 0) {

            handleMissing match {

              case HandleMissing.Impute() =>

                logImputedFields(nMissing, missingFields)

                for {
                  missingSchema <- StructType(missingFields).runTask
                  fullSchema    <- (frameSchema ++ missingSchema).runTask
                  ds <- L.crossRows(frame)(NonEmptyChunk(emptyRow(missingSchema)))
                } yield DefaultLeapFrame(fullSchema, ds)

              case HandleMissing.Error() => Task.fail(new TransformError(
                s"Missing required fields: [${missingFields.mkString(", ")}]", ""))
            }
          } else {
            L.flatten(frame)
          }
        }.flatten

        private def logImputedFields(nMissing: Int, missingFields: Seq[StructField]): Unit = {

          val logFields = if (nMissing > debugMissingFieldLimit) {
            missingFields.take(debugMissingFieldLimit).map(_.name).mkString(", ") + ", ..."
          } else {
            missingFields.map(_.name).mkString(", ")
          }

          logger.info(s"Imputing $nMissing missing fields: [$logFields]")
        }

        private def createTransform(crossedSchema: StructType,
                                    select: Option[NonEmptyList[String]])
        : Try[(StructType, Row => Task[Row])] = Try {

          val selectInFields = crossedSchema.indicesOf(inFields: _*).get

          val requestFields = crossedSchema.fields.flatMap(Option(_).filterNot(s => outFields.contains(s.name)))
          val requestFieldsLen = requestFields.length

          val outSchema = StructType(outputSchema.fields ++ requestFields).get

          val selectedSchema = select.fold(outSchema)(fields => outSchema.select(fields.toList: _*).get)

          val castTransforms = createCastTransforms(
            inputSchema   = crossedSchema,
            targetSchema  = inputSchema).get

          val cast: Row => ArrayRow = {
            case row: ArrayRow =>
              castTransforms.foreach { case (name, cast) =>
                val idx = inputSchema.indexOf(name).get
                row.set(idx, cast(row.getRaw(idx)))
              }
              row
            case row =>
              val arr = row.toArray
              castTransforms.foreach { case (name, cast) =>
                val idx = inputSchema.indexOf(name).get
                arr.update(idx, cast(arr(idx)))
              }
              ArrayRow(arr)
          }

          val selector: Row => Row = select match {
            case Some(fields) =>
              val f = outSchema.indicesOf(fields.toList: _*).get
              _.selectIndices(f: _*)
            case None => identity
          }

          selectedSchema -> ((row: Row) => Task {

            val castedRow = cast(row.selectIndices(selectInFields: _*))

            val arr = new Array[Any](rowTransformer.maxSize + requestFieldsLen)

            castedRow.toArray.copyToArray(arr)

            /**
             * RowTransformer does not retain any fields post-transformation that are not contained
             * in it's output schema, so we have to add those fields back after transformation
             */
            requestFields
              .map(f => row.getRaw(crossedSchema.indexOf(f.name).get))
              .copyToArray(arr, rowTransformer.maxSize)

            val arrRow = ArrayRow(values = arr)

            val transformedRow = ChunkedRow(Chunk.fromArray(rowTransformer
              .transforms
              .foldLeft(Option(arrRow)) { (r, transform) => r.flatMap(transform) }
              .get.values.array))

            selector(transformedRow)
          })
        }
      }

      object InMemoryTransformer {

        def apply(transformer: Transformer): RIO[Blocking, InMemoryTransformer] =
          transformer.transform(RowTransformer(transformer.inputSchema))
            .runBlockingTask
            .mapEffect(new InMemoryTransformer(_, toModelOpTree(transformer).value))
      }
    }
  }
}