package exd.fundamenski.skynet.util
package render

import reftree.diagram.Diagram

import scala.util.control.NonFatal

object App {

  import reftree.render._

  val tmp = os.rel / "animation"

  val op = mleap.readModelOp("file:/Users/adrielcasellas/IdeaProjects/skynet/skynet-api/models/xgb-ltr.model").get

  implicit val conf = OpExpr.ModelGraphOptions(elideSchemaForParent = false)

  val animation = Animate.animate(op)

  println(tmp.toNIO)

  val renderer = Renderer(
    renderingOptions = RenderingOptions(verticalSpacing = 0.5, density = 50),
    directory = tmp.toNIO,
    format = "svg"
  )

  import renderer._

  val t = System.nanoTime().toString

//  println(Shortcuts.svg(t).toString())
//
//  try {
//    os.write(os.Path((tmp / "xmltext.xml").toNIO), Shortcuts.svg(t).text)
//  } catch { case NonFatal(e) =>
//    println("XML error: " + e.getMessage)
//  }
//
  try {
    Diagram(op).render(t)
  } catch { case NonFatal(e) =>
    println("Diagram error: " + e.getMessage)
  }

  val str = os.read(os.Path((tmp / (t + ".svg")).toNIO.toAbsolutePath))
  scala.xml.parsing.XhtmlParser(scala.io.Source.fromString(str)).toString()


//  try {
//    animation.render(t)
//  } catch { case NonFatal(e) =>
//    println("Animation error: " + e.getMessage)
//  }
}
