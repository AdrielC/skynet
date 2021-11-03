package exd.fundamenski.skynet.util.render

import exd.fundamenski.skynet.util.mleap.StructOps
import cats.data.NonEmptyVector
import cats.{Applicative, Traverse}
import higherkindness.droste.util.DefaultTraverse
import cats.implicits._
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType, TensorType}
import reftree.core.RefTree
import reftree.core._

sealed trait OpExpr[+V, +A] extends Product with Serializable
object OpExpr {

  implicit def traverseExpr[V]: Traverse[OpExpr[V, *]] =
    new DefaultTraverse[OpExpr[V, *]] {
      def traverse[G[_]: Applicative, A, B](fa: OpExpr[V, A])(f: A => G[B]): G[OpExpr[V, B]] = fa match {
        case o: Node[V, A]  => o.children.map(f) match {
          case Some(value)  => value.map(b => Node[V, B](o.value, children = Some(b)))
          case None         => (o.copy(children = none[B]): OpExpr[V, B]).pure[G]
        }
        case o: Par[V, A] => (o.ops.traverse(f), o.children.traverse(f))
          .mapN((ops, ch) => Par[V, B](ops, children = ch))

        case Root(a) => f(a).map(Root[V, B])
      }
    }

  case class Root[+V, A](root: A) extends OpExpr[V, A]
  object Root {

    implicit def refTreeRoot[V, A](implicit R: ToRefTree[A]): ToRefTree[Root[V, A]] = root =>
      RefTree.Ref(
        root,
        List(root.root.refTree.toField))
  }

  case class Par[+V, +A](ops: NonEmptyVector[A], children: Option[A] = None) extends OpExpr[V, A]
  object Par {

    implicit def refTreePar[V, A](implicit R: ToRefTree[A]): ToRefTree[Par[V, A]] = ops => {
      val o = ops.ops.toVector.map(_.refTree.toField)
      val oo = ops.children match {
        case Some(value) => o :+ value.refTree.toField.withName("next")
        case None => o
      }
      RefTree.Ref(ops, oo)
    }
  }

  case class Node[+V, +A](value: V, children: Option[A] = None) extends OpExpr[V, A]
  object Node {

    implicit def nodeToRefTree[A]
    (implicit R: ToRefTree[A], C: ModelGraphOptions): ToRefTree[Node[Op.Description, A]] = {

      case n @ Node(Op.Description(name, opName, inputSchema, outputSchema), c) =>


        def schemaToField(schema: StructType, treeHighlight: Boolean): List[RefTree.Ref.Field] =
          schema.fields.toList.map(structFieldToRefTree(_, treeHighlight))

        def structFieldToRefTree(structField: StructField, treeHighlight: Boolean): RefTree.Ref.Field = {
          val ref =
              RefTree.Val(structField.dataType.asInstanceOf[AnyVal])
                .withFormattedValue(structField.dataType match {
                  case ScalarType(base, _) => s"$base"
                  case t @ TensorType(base, dimensions, _) =>
                    s"${t.simpleString}($base${dimensions
                      .filter(_.nonEmpty)
                      .map(d => s", [${d.mkString(",")}]")
                      .getOrElse("")})"
                  case l => s"${l.simpleString}(${l.base})"
                })
                .withName("type")

          (if (C.uniqueSchemas) ref else ref.withId(structField.name))
            .withHighlight(treeHighlight)
            .toField
            .withName(structField.name)
        }

        val in = schemaToField(inputSchema, treeHighlight = false)

        val out = schemaToField(outputSchema, treeHighlight = true)

        val fieldsWithChild = c.map(_.refTree.toField.withName("next")) match {
          case Some(value) =>
            if (name.equalsIgnoreCase("Pipeline")) {
              if (C.elideSchemaForParent) value :: Nil else in :+ value
            } else if (name.equalsIgnoreCase("VectorAssembler")) {
              List(RefTree.Ref(inputSchema.asInstanceOf[AnyRef],
                schemaToField(inputSchema, treeHighlight = false)
              ).toField, out.head) :+ value
            } else {
              in :+ value
            }
          case None => in ++ out
        }

        RefTree.Ref(n, fieldsWithChild).withName(s"$name: $opName")
    }
  }

  /**
   *
   * @param elideSchemaForParent whether the schema for parent nodes should be elided
   * @param uniqueSchemas whether the ids for each field are unique or shared among nodes
   */
  case class ModelGraphOptions(elideSchemaForParent: Boolean = false,
                               uniqueSchemas: Boolean = false)
  object ModelGraphOptions {

    object default {
      implicit val defaultConfig: ModelGraphOptions = ModelGraphOptions()
    }
  }
}