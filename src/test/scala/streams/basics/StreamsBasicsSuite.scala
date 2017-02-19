package streams.basics

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class StreamsBasicsSuite extends FunSuite {
  test("Smote test"){
    assert(true)
  }

  test("Se puede construir un Source a partir de un Iterable cualquiera"){
    /*Se puede crear un Source con su apply por defecto que toma un Iterable.
    El NotUsed significa que es un Source no materializado aun
    */
    val source: Source[Int, NotUsed] = Source(1 to 10)
    /*Como estamos probando un Source vamos por ahora a hacer un flujo de datos
    que arranca en source y se va a un Sink que ignora los datos. Debemos entocnes
    crear el sink para luego unirlo con el source.
    */
    val sink: Sink[Any, Future[Done]] = Sink.ignore
    /*Un source se debe unir a un sink para luego poder ejecutarse. Notar como
    el tipo de source.to(sink) es un grafo y que adicionalmente esta parametrizado a NotUsed
    pues se trata simplemente de la descripcion de un flujo y no su ejecucion (aka su materializacion)
    */
    val g: RunnableGraph[NotUsed] = source.to(sink)
    /*Un grafo se materializa con run. Es importante ver que para poder ejecutar un grafo siempre hay que proveer
    ActorSystem y un ActorMaterializer de forma implicita
    */
    implicit val system = ActorSystem("SystemForTestingAkkaStreams")
    implicit val materializer = ActorMaterializer()
    val res: NotUsed = g.run()
    /*Este test solo demuestra que es lo minimo necesario para construir un
    grafo que compile. Dado que el Sink es ignore no podemos realizar
    ninguna verificacion
    */
    assert(true)
  }

  test("Unir un source a un sink que haga algo"){
    /*Los implicits siempre necesarios para poder ejecutar un grafo*/
    implicit val system = ActorSystem("SystemForTestingAkkaStreams")
    implicit val materializer = ActorMaterializer()

    val source: Source[Int, NotUsed] = Source(1 to 10)

    /*Esta vez tenemos un Sink que hace algo. Cuando le lleguen datos
    * los va a foldear (acumular) con la operacion binaria + */
    val sink: Sink[Int, Future[Int]] = Sink.fold(0)(_+_)

    /*Notar dos cosas:
    * 1. Que esta vez el to cambiar por toMat
    * 2. Que r1 no tiene el keepRight al final. Por esto, no hay nugrafo para ejecutar*/
    val r1: ((NotUsed, Future[Int]) => Nothing) => RunnableGraph[Nothing] = source.toMat(sink)

    /*r1 si tiene Keep.right y es por eso que evalua a grafo y se puede ejecutar posteriormente*/
    val r2: RunnableGraph[Future[Int]] = source.toMat(sink)(Keep.right)

    /*La ejecucion del grafo es lo que dice la intuicion. Toma el source
    * que son los numeros del 1 al 10 y los lleva a un Sink que los va a acumular
    * y entrega el valor de la suma en un Future. Esto denota que el futuro se completa
    * cuando el stream termine*/
    val resFut1: Future[Int] = r2.run()

    /*Esperamos por el futuro (porque estamos en tests y lo podemos hacer)
    * y verificamos que el valor final sea la suma de los numeros del 1 l 10*/
    val res1 = Await.result(resFut1, Duration.Inf)
    assertDoesNotCompile("r1.run()")
    val valorEsperado = 1+2+3+4+5+6+7+8+9+10
    assert(res1== valorEsperado)

    /*Finalmente vemos que para evitar la sintaxis un poco confusa del Keep.right
    * se puede hacer dos pasos al mismo tiempo:
    * 1. El enlace con sink (que recien lo hicimos con .to)
    * 2. La ejecucion del stream (que recien la hicimos con .run)
    * .runWith hace las dos cosas al tiempo*/
    val resFut2 = source.runWith(sink)

    /*Esperamos resultado y verificamos que de lo mismo que la ejecucion previa*/
    val res2 = Await.result(resFut2, Duration.Inf)
    assert(res2 == valorEsperado)
  }
}
