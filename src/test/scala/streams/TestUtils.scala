package streams

import scala.concurrent._
import scala.concurrent.duration._

object TestUtils {

  def await[T](f:Future[T]): T = {
    Await.result(f, Duration.Inf)
  }
}
