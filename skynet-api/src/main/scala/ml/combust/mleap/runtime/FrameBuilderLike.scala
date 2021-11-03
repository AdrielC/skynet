package ml.combust.mleap.runtime

import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.FrameBuilder
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
import zio.{Task, UIO}

import java.io.PrintStream

trait FrameBuilderLike[A] {
  /** Get the schema.
   *
   * @return schema
   */
  def schema(a: A): StructType

  /** Try to select fields to create a new LeapFrame.
   *
   * Returns a Failure if attempting to select any fields that don't exist.
   *
   * @param fieldNames field names to select
   * @return try new LeapFrame with selected fields
   */
  def select(a: A)(fieldNames: String *): Task[A]

  /** Selects all of the fields from field names that exist in the leap frame.
   * Returns a new leap frame with all of the available fields.
   *
   * @param fieldNames fields to try and select
   * @return leap frame with select fields
   */
  def relaxedSelect(a: A)(fieldNames: String *): UIO[A] = {
    val actualFieldNames = fieldNames.filter(schema(a).hasField)
    select(a)(actualFieldNames: _*).orDie
  }

  /** Try to add a column to the LeapFrame.
   *
   * Returns a Failure if trying to add a field that already exists.
   *
   * @param name name of column
   * @param selectors row selectors used to generate inputs to udf
   * @param udf user defined function for calculating column value
   * @return LeapFrame with new column
   */
  def withColumn(a: A)
                (name: String, selectors: Selector *)
                (udf: UserDefinedFunction): Task[A]

  /** Try to add multiple columns to the LeapFrame.
   *
   * Returns a Failure if trying to add a field that already exists.
   *
   * @param names names of columns
   * @param selectors row selectors used to generate inputs to udf
   * @param udf user defined function for calculating column values
   * @return LeapFrame with new columns
   */
  def withColumns(a: A)
                 (names: Seq[String], selectors: Selector *)
                 (udf: UserDefinedFunction): Task[A]

  /** Try dropping column(s) from the LeapFrame.
   *
   * Returns a Failure if the column does not exist.
   *
   * @param names names of columns to drop
   * @return LeapFrame with column(s) dropped
   */
  def drop(a: A)(names: String *): Task[A]

  /** Try filtering the leap frame using the UDF
   *
   * @param selectors row selectors used as inputs for the filter
   * @param udf filter udf, must return a Boolean
   * @return LeapFrame with rows filtered
   */
  def filter(a: A)
            (selectors: Selector *)
            (udf: UserDefinedFunction): Task[A]

  /** Print the schema to standard output.
   */
  def printSchema(a: A): Unit = schema(a).print(System.out)

  /** Print the schema to a PrintStream.
   *
   * @param out print stream to print schema to
   */
  def printSchema(a: A)(out: PrintStream): Unit = schema(a).print(out)
}

object FrameBuilderLike {

  implicit def fbLike[FB <: FrameBuilder[FB]]: FrameBuilderLike[FB] = new FBLike[FB] {}

  trait FBLike[FB <: FrameBuilder[FB]] extends FrameBuilderLike[FB] {

    override def schema(a: FB): StructType = a.schema

    override def select(a: FB)(fieldNames: String*): Task[FB] =
      Task.fromTry(a.select(fieldNames: _*))

    override def withColumn(a: FB)(name: String, selectors: Selector*)(udf: UserDefinedFunction): Task[FB] =
      Task.fromTry(a.withColumn(name, selectors: _*)(udf))

    override def withColumns(a: FB)(names: Seq[String], selectors: Selector*)(udf: UserDefinedFunction): Task[FB] =
      Task.fromTry(a.withColumns(names, selectors: _*)(udf))

    override def drop(a: FB)(names: String*): Task[FB] =
      Task.fromTry(a.drop(names: _*))

    override def filter(a: FB)(selectors: Selector*)(udf: UserDefinedFunction): Task[FB] =
      Task.fromTry(a.filter(selectors: _*)(udf))
  }
}

