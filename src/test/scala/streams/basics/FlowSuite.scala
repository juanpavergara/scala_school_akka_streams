package streams.basics

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.FunSuite
import akka.stream.scaladsl._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class FlowSuite extends FunSuite {

  /*Vamos a usar en esta suite estos val para todos los tests*/
  implicit val system = ActorSystem("SystemForTestingAkkaStreams")
  implicit val materializer = ActorMaterializer()
  val source: Source[Int, NotUsed] = Source(1 to 10)
  val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_+_)

  test("Smoke test"){
    assert(true)
  }

  test("Crear un flow a partir de una funcion con notacion infija"){
    /*Funcion comun y corriente a partir de la cual se puede crear un flow*/
    def twice(i:Int) = i*2
    /*Creacion de un flow con fromFunction*/
    val flow1 = Flow.fromFunction(twice)
    /*Es posible usar notacion infija dado que la funcion requiere un solo parametro.
    * Asi se podria ver como que se saca provecho del dsl de streams*/
    val flow2 = Flow fromFunction twice

    /*Notar como se puede construir el grafo con notacion infija tambien*/
    val resFut1 = source via flow1 runWith sink
    val resFut2 = source via flow2 runWith sink

    val res1 = Await.result(resFut1, Duration.Inf)
    val res2 = Await.result(resFut2, Duration.Inf)

    assert(res1 == 110)
    assert(res2 == 110)

  }

  test("Se puede ejecutar un grafo a partir de flow indicando el Source y el Sink"){
    val flow1 = Flow[Int].map(_*2)
    /*runWith en un Flow entrega como resultado una tupla con la materizalizacion del Source y del Sink*/
    val res: (NotUsed, Future[Int]) = flow1.runWith(source, sink)
    val r = Await.result(res._2, Duration.Inf)
    assert(r == 110)
  }

  test("Se pueden unir dos flujos y quedar en un solo Flujo"){

    def serialize(i:Int) = i.toString
    def concatQuestionMark(s:String) = s"${s}?"

    def flow1 = Flow.fromFunction(serialize)
    def flow2 = Flow.fromFunction(concatQuestionMark)

    /*Notar como resulta un flujo que recibe entero y bota String*/
    def flow3: Flow[Int, String, NotUsed] = Flow[Int].via(flow1).via(flow2)

    val sinkString: Sink[String, Future[String]] = Sink.fold("")(_+_)

    /*El siguiente grafo materializado no compila porque sink recibe Ints y flow3 recibe Int y arroja String*/
    assertDoesNotCompile("source via flow3 runWith sink")

    val resF: Future[String] = source via flow3 runWith sinkString

    val res = Await.result(resF, Duration.Inf)

    println(res)
    val resEsperado = "1?2?3?4?5?6?7?8?9?10?"

    assert(res == resEsperado)

  }


}
