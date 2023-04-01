package ml.combust.mleap.runtime

import com.overstock.skynet.util.mleap
import com.overstock.skynet.util.mleap.{RowOps, StructOps}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.FrameBuilderLike.FBLike
import zio.console.{Console, putStrLn}
import zio.{RIO, Task, ZIO}

trait LeapFrameLike[A] extends FrameBuilderLike[A] { self =>

  def flatten(a: A): Task[frame.DefaultLeapFrame]

  def zip[B](fb: A, other: B)(implicit L: LeapFrameLike[B]): Task[frame.DefaultLeapFrame] =
    ZIO.fromTry(schema(fb) ++ L.schema(other)).flatMap { schema =>
      collect(fb).zipWithPar(L.collect(other))((a, b) =>
        frame.DefaultLeapFrame(schema, (a zip b).map(((_: frame.Row) concat (_: frame.Row)).tupled)))
    }

  def cross[Other](fb: A, other: Other)(implicit L: LeapFrameLike[Other]): Task[frame.DefaultLeapFrame] = {
    val fSchema = schema(fb)
    val oSchema = L.schema(other)
    ZIO.fromTry(fSchema ++ oSchema).flatMap { schema =>
      self.crossRows(fb, other).map(frame.DefaultLeapFrame(schema, _))
    }
  }

  def crossRows[Other](fb: A, other: Other)(implicit L: LeapFrameLike[Other]): Task[Seq[frame.Row]] =
    Task.mapN(collect(fb), L.collect(other))(mleap.crossRows(schema(fb), _, _))

  def crossRows(fb: A)(otherRows: Seq[frame.Row]): Task[Seq[frame.Row]] =
    collect(fb).mapEffect(mleap.crossRows(schema(fb), _, otherRows))

  def prefix(fb: A)(colPrefix: String): Task[frame.DefaultLeapFrame] =
    collect(fb).flatMap(rows => Task(frame.DefaultLeapFrame(
      StructType(schema(fb).fields.map(f => f.copy(colPrefix + f.name))).get,
      rows
    )))

  /** Collect all rows into a Seq
   *
   * @return all rows in the leap frame
   */
  def collect(a: A): Task[Seq[frame.Row]]

  /** Print this leap frame
   *
   */
  def show(a: A, n: Int = 20, truncate: Int = 20): RIO[Console, Unit] = {

    def showString: Task[String] = {

      val schema = self.schema(a)
      collect(a).map { dataset =>
        val rows = schema.fields.map(_.name) +: dataset.take(n).map {
          _.map {
            cell =>
              val str = if (cell != null) cell match {
                case v: Option[_] => v.map(_.toString).getOrElse("null")
                case v: Seq[_] => v.mkString("[", ",", "]")
                case v => v.toString
              } else "null"

              if (truncate > 0 && str.length > truncate) {
                // do not show ellipses for strings shorter than 4 characters.
                if (truncate < 4) str.substring(0, truncate)
                else str.substring(0, truncate - 3) + "..."
              } else {
                str
              }
          }
        }

        val sb = new StringBuilder()
        val numCols = schema.fields.size

        // Initialise the width of each column to a minimum value of '3'
        val colWidths = Array.fill(numCols)(3)

        // Compute the width of each column
        for(row <- rows) {
          for((cell, i) <- row.zipWithIndex) {
            colWidths(i) = math.max(colWidths(i), cell.length)
          }
        }

        // Create SeparateLine
        val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

        // column names
        rows.head.zipWithIndex.map {
          case (cell, i) =>
            if (truncate > 0) {
              leftPad(cell, colWidths(i))
            } else {
              rightPad(cell, colWidths(i))
            }
        }.addString(sb, "|", "|", "|\n")

        sb.append(sep)

        // data
        rows.tail.map {
          _.zipWithIndex.map {
            case (cell, i) =>
              if (truncate > 0) {
                leftPad(cell, colWidths(i))
              } else {
                rightPad(cell, colWidths(i))
              }
          }.addString(sb, "|", "|", "|\n")
        }

        sb.append(sep)
        sb.toString
      }
    }

    def leftPad(str: String, padTo: Int, c: Char = ' '): String = {
      val n = padTo - str.length

      if(n > 0) {
        (0 until n).map(_ => c).mkString("") + str
      } else { str }
    }

    def rightPad(str: String, padTo: Int, c: Char = ' '): String = {
      val n = padTo - str.length

      if(n > 0) {
        str + (0 until n).map(_ => c).mkString("")
      } else { str }
    }

    showString >>= (putStrLn(_))
  }
}

object LeapFrameLike {

  implicit def lfLike[LF <: frame.LeapFrame[LF]]: LeapFrameLike[LF] = new LeapFrameLike[LF] with FBLike[LF] {
    override def flatten(a: LF): Task[frame.DefaultLeapFrame] = Task {
      frame.DefaultLeapFrame(
        a.schema,
        a.collect()
      )
    }

    override def collect(a: LF): Task[Seq[frame.Row]] = Task(a.collect())
  }
}