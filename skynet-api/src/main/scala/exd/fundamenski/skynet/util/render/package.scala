package exd.fundamenski.skynet.util

import exd.fundamenski.skynet.util.render.Op.Fixed
import cats.data.NonEmptyVector
import higherkindness.droste.data.Fix
import higherkindness.droste.{Algebra, scheme}
import higherkindness.droste.syntax.all._
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import reftree.core.{RefTree, RefTreeSyntax, ToRefTree}
import cats.Functor

import cats.implicits._

package object render {

  implicit object opFunctor extends Functor[Op.Fixed] {

    private def algebra[A, B](f: A => B): Algebra[OpExpr[A, *], Op.Fixed[B]] = Algebra {
      case OpExpr.Node(value, children) => Op.Node(f(value), children)
      case other: OpExpr.Par[B, Op.Fixed[B]] @unchecked => (other: OpExpr[B, Op.Fixed[B]]).fix
      case other: OpExpr.Root[B, Op.Fixed[B]] @unchecked => (other: OpExpr[B, Op.Fixed[B]]).fix
    }

    override def map[A, B](fa: Fixed[A])(f: A => B): Fixed[B] =
      scheme.cata(algebra(f)).apply(fa)
  }

  implicit val toRefTreeRefTree: ToRefTree[RefTree] = identity

  implicit def toRefTreeFix[F[_]: Functor](implicit T: ToRefTree[F[RefTree]]): ToRefTree[Fix[F]] = { a =>
    scheme.cata(Algebra[F, RefTree] { a => T.refTree(a) }).apply(a)
  }

  implicit class OpOps[V](private val op: Op.Fixed[V]) extends AnyVal {

    def andThen(next: Op.Fixed[V]): Op.Fixed[V] = op.unfix match {
      case seq: OpExpr.Par[V, Op.Fixed[V]] => Op.Par(seq.ops, seq.children.map(_.andThen(next)) orElse Some(next))
      case root: OpExpr.Root[V, Op.Fixed[V]] => Op.Root(root.root.andThen(next))
      case node: OpExpr.Node[V, Op.Fixed[V]] => Op.Node(node.value, node.children.map(_.andThen(next)) orElse Some(next))
    }

    def par(next: Op.Fixed[V]): Op.Fixed[V] = (op.unfix, next.unfix) match {
      case (OpExpr.Par(ops, c1), OpExpr.Par(ops2, c2)) => Op.Par(ops concatNev ops2,
        (c1, c2).mapN((a, b) => OpOps[V](a) andThen b))
      case (ops, OpExpr.Par(ops2, c)) => Op.Par(ops2.prepend(ops.fix), c)
      case (OpExpr.Par(ops, c), ops2) => Op.Par(ops.append(ops2.fix), c)
      case _ => Op.Par(NonEmptyVector.of(op, next), None)
    }
  }

  type Op = Fix[OpExpr[Op.Description, *]]
  object Op {
    type Fixed[A] = Fix[OpExpr[A, *]]

    case class Description(
      name: String,
      opName: String,
      inputSchema: StructType,
      outputSchema: StructType
    )

    def Root[V](root: Fixed[V]): Fixed[V] = Fix(OpExpr.Root[V, Fixed[V]](root))
    def Par[V](ops: NonEmptyVector[Fixed[V]], c: Option[Fixed[V]]): Fixed[V] = Fix(OpExpr.Par(ops, c))
    def Node[V](value: V, children: Option[Op.Fixed[V]]): Op.Fixed[V] = Fix(OpExpr.Node(value, children))
    def Description(name: String,
                    opName: String,
                    inputSchema: StructType,
                    outputSchema: StructType,
                    children: Option[Op]): Op =
      Node(Description(name, opName, inputSchema, outputSchema), children)
  }

  implicit val refTree: ToRefTree[StructType] = schema =>
    RefTree.Ref(
      schema,
      schema.fields.map(f => f.refTree.withId(schema.fields.hashCode() + f.name).toField))

  implicit val toRefTreeStructField: ToRefTree[StructField] = { s =>
    RefTree.Ref(s,
      Seq(
        RefTree.Val(s.dataType.asInstanceOf[AnyVal])
          .withFormattedValue(
            s.dataType match {
              case ScalarType(base, isNullable) => if (isNullable) s"$base" else s"$base(nullable)"
              case l => if (l.isNullable) s"${l.simpleString}(${l.base})" else s"${l.simpleString}(${l.base})"
            }
          )
          .toField
          .withName("type"))
    ).copy(name = s.name)
  }

  implicit class RefTreeOps(private val refTree: RefTree) extends AnyVal {
    def withId(id: String): RefTree = refTree match {
      case r: RefTree.Ref => r.copy(id = id)
      case other => other
    }

    def withName(name: String): RefTree = refTree match {
      case r: RefTree.Ref => r.copy(name = name)
      case other => other
    }
  }
}
