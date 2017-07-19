package streams.basics

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Executors

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Future}


class StreamsWithFutureSuite extends FunSuite{

  val sdf = new SimpleDateFormat("yyyy/mm/dd hh:mm:ss:SS")

  def log(s:String) = {
    println(s"[${sdf.format(new Date())}][${Thread.currentThread().getName}] ${s}")
  }

  ignore("How does Akka Streams handle Future?") {

    object ecs{
      val ec1 = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
      val ec2 = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
    }

    implicit val system = ActorSystem("SystemForTestingAkkaStreams")
    implicit val materializer = ActorMaterializer()

    def futurize(i: Int): Future[Int] = {
      implicit val ec2 = ecs.ec2
      Future{
        log(s"Futurizing $i")
        Thread.sleep(2000)
        i
      }(ec2)
    }

    def source = Source(1 to 10)

    def flow = Flow[Int].mapAsync(1)(futurize)

    val g: Future[Done] = source
      .via(flow)
      .runWith(
        Sink.foreach(x => log(s"$x"))
      )

  }

  test("Now with futures without streams"){

    object ecs{
      val ec1 = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
      val ec2 = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    }

    def foo(i:Int) = {
      implicit val ec = ecs.ec1
      Future{
        log(s"Futurizing $i")
        Thread.sleep(1000)
        i
      }
    }


    implicit val ec = ecs.ec2
    def bar(i:Int)= for {
      a <- {
        log(s"About to futurize $i")
        foo(i)
      }
    } yield a + 1

    (1 to 10).foreach{
      bar(_)
    }

  }

}
