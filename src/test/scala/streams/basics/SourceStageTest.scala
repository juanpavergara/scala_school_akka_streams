package streams.basics

import scala.util.Random

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}


class SourceStageTest extends GraphStageWithMaterializedValue[SourceShape[Int], String] {

  val out: Outlet[Int] = Outlet[Int]("SourceStageTest.out")

  override def shape: SourceShape[Int] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, String) = {
    val logic = new GraphStageLogic(shape) with OutHandler {
      val random = new Random()
      var counter = 0

      override def onPull(): Unit = {
        val rand = random.nextInt()
        if (counter < 10) {
          counter += 1
          push(out, rand)
        } else
          completeStage()
      }
      setHandler(out, this)
    }
    (logic, "TERMINADO")
  }

}
