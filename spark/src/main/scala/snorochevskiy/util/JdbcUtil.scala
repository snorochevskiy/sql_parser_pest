package snorochevskiy.util

import java.sql._

object JdbcUtil {

  def validateRdbmsQuery(connectionUrl: String, user: String, password: String, query: String): Either[String, Unit] = {
    val con: Connection = DriverManager.getConnection(connectionUrl, user, password)
    try {
      val stmt = con.createStatement()
      val rs = stmt.executeQuery(s"DESCRIBE ${query}")
      rs.close()
      stmt.close()
      Right(())
    } catch {
      case e: SQLException =>
        Left(e.getMessage)
    } finally {
      con.close()
    }
  }

}
