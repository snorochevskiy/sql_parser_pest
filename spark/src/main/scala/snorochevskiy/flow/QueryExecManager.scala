package snorochevskiy.flow

import org.apache.spark.sql.SparkSession
import snorochevskiy.web.servlet.QueueExecElement

import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue}

class QueryExecManager(spark: SparkSession) {

  val queue = new LinkedBlockingQueue[QueueExecElement](10)

  def start() = {
    val executor = Executors.newSingleThreadExecutor()
    executor.submit(new Runnable {
      override def run(): Unit = {

      }
    })
  }

}
