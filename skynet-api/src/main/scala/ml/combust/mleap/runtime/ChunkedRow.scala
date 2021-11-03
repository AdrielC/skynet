package ml.combust.mleap.runtime

import zio.Chunk

case class ChunkedRow(chunks: Chunk[Any]) extends frame.Row {
  override def selectIndices(indices: Int*): frame.Row = ChunkedRow(Chunk.fromIterable(indices).map(chunks))
  override def iterator: Iterator[Any] = chunks.iterator
  override def dropIndices(indices: Int*): frame.Row = ChunkedRow {
    val drops = indices.toSet
    chunks.zipWithIndex.collect { case (a, idx) if !drops.contains(idx) => a }
  }
  override def getRaw(index: Int): Any = chunks(index)
  override def withValues(values: Seq[Any]): frame.Row = ChunkedRow(chunks ++ Chunk.fromIterable(values))
  override def withValue(value: Any): frame.Row = ChunkedRow(chunks :+ value)

  def ++(row: frame.Row): ChunkedRow = row match {
    case frame.ArrayRow(values) => ChunkedRow(chunks ++ Chunk.fromArray(values.array))
    case ChunkedRow(c)          => ChunkedRow(chunks ++ c)
    case _                      => ChunkedRow(chunks ++ Chunk.fromArray(row.toArray))
  }
}

object ChunkedRow {

  def apply(values: Any*): frame.Row = ChunkedRow(Chunk.fromIterable(values))
}
