package snorochevskiy.app

import org.sparkproject.jetty.server.{Server, ServerConnector}
import org.sparkproject.jetty.servlet.{ServletHandler, ServletHolder}
import snorochevskiy.config.ConfigProvider
import snorochevskiy.spark.SparkProvider
import snorochevskiy.web.servlet.SqlExecHttpServlet

trait ServerSqlExecutionApp {
  this: SparkProvider =>

  def main(args: Array[String]): Unit = appMain(args)

  def appMain(args: Array[String]): Unit = {
    val cfg = ConfigProvider.loadAppConf()

    val server = new Server()
    val connector = new ServerConnector(server)
    connector.setPort(8097)
    server.setConnectors(Array(connector))

    val handler = new ServletHandler()

    val servletHolder = new ServletHolder(new SqlExecHttpServlet(spark, cfg))
    handler.addServletWithMapping(servletHolder, "/*")

    server.setHandler(handler)

    server.setAttribute("spark", spark)

    server.start()
    server.join()
  }

}
