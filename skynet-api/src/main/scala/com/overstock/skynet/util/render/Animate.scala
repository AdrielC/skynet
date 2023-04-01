package com.overstock.skynet.util.render

import higherkindness.droste.{AlgebraM, scheme}
import reftree.diagram.{Animation, Diagram}
import cats.implicits._
import reftree.core.{RefTree, ToRefTree}

object Animate {

  def animate[V](op: Op.Fixed[V])(implicit T: ToRefTree[OpExpr.Node[V, (String, RefTree)]]): Animation =
    Animation.Builder {
      scheme.cataM(algebra[V]).apply(op)
    }.build(op => Diagram.Single(op._2).withCaption(op._1))

  def algebra[V](implicit T: ToRefTree[OpExpr.Node[V, (String, RefTree)]])
  : AlgebraM[Vector, OpExpr[V, *], (String, RefTree)] =
    AlgebraM {
      case OpExpr.Root(root) => Vector("Root" -> root.refTree)
      case OpExpr.Par(children, c) => children.toVector ++ c.toVector
      case o @ OpExpr.Node(Op.Description(_, name, _, _), _) => Vector(name -> o.refTree)
      case o @ OpExpr.Node(value, children) => children.fold(Vector("Op Node" -> o.refTree)) {
        case (name, tree) =>
          o.refTree match {
            case n @ RefTree.Null(highlight) => Vector(name -> tree)
            case v @ RefTree.Val(value, formattedValue, highlight) => Vector("Op Node" -> o.refTree)
            case RefTree.Ref(name, id, children, highlight) => Vector(name ->
              RefTree.Ref(name, id, tree.toField.withName(name) +: children, highlight))
          }
      }
    }
}