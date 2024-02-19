package snorochevskiy.util


import java.io.{DataOutputStream, IOException, InputStream}
import java.net.{HttpURLConnection, URL}
import java.util.{Base64, Date}

import scala.io.Source
import scala.util.Try

sealed trait AbstractAuth
case class GitTokenAuth(token: String) extends AbstractAuth
case class BasicAuth(login: String, password: String) extends AbstractAuth
case class WebSecAuth(url: String, clientId: String, secret: String, scope: String) extends AbstractAuth
case object NoAuth extends AbstractAuth

case class HttpResponse(code: Int, payload: String) {
  def isSuccess(): Boolean = code >= 200 && code < 300
}

/**
 * Using this old java.net.URL wrapper to not introduce additional dependencies.
 * We need to try to keep the artifact as light as possible.
 */
object HttpUtil {

  def get(url: String, auth: Option[AbstractAuth]): String Either String = {

    def getWithAuth(url: String, auth: Option[AbstractAuth], clearCache: Boolean): HttpResponse = {
      var headers: List[(String,String)] = Nil

      auth match {
        case Some(BasicAuth(login, password)) =>
          val base64Creds = "Basic " + new String(Base64.getEncoder.encode(s"${login}:${password}".getBytes))
          headers = "Authorization" -> base64Creds :: headers
        case Some(WebSecAuth(url, clientId, secret, scope)) =>
          obtainWebSecToken(url, clientId, secret, scope, clearCache) match {
            case Right(token) => headers = "Authorization" -> token :: headers
            case Left(httpResponse) => return httpResponse
          }
        case _ => ()
      }

      HttpUtil.get(url, headers)
    }

    var resp = getWithAuth(url, auth, false)
    if (!resp.isSuccess() && resp.code == 401) {
      resp = getWithAuth(url, auth, true)
    }

    if (resp.isSuccess()) Right(resp.payload) else Left(s"${resp.code}: ${resp.payload}")
  }

  def get(url: String, headers: List[(String,String)]): HttpResponse = {
    httpCall("GET", url, headers, None)
  }

  def post(url: String, headers: List[(String,String)], payload: String): HttpResponse = {
    httpCall("POST", url, headers, Some(payload))
  }

  def httpCall(method: String, url: String, headers: List[(String,String)], payload: Option[String]): HttpResponse = {
    val conn = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)

    conn.setRequestMethod(method)

    headers.foreach { case (name, value) =>
      conn.setRequestProperty(name, value)
    }

    payload.foreach { p =>
      conn.setDoOutput(true)
      val pw = new DataOutputStream(conn.getOutputStream)
      pw.writeBytes(p)
      pw.flush()
      pw.close()
    }

    var in: InputStream = null
    try {
      in = conn.getInputStream
      val source = Source.fromInputStream(in)
      val text = source.mkString
      source.close()
      HttpResponse(conn.getResponseCode, text)
    } catch {
      // java.net.UnknownHostException: host-that-doesnt-exist.com
      case e: java.net.UnknownHostException => HttpResponse(404, e.getMessage)

      // java.io.IOException: Server returned HTTP response code: 401 for URL: https://host/path
      case e: IOException => HttpResponse(conn.getResponseCode, e.getMessage)

      case e: Exception => HttpResponse(404, e.getMessage)
    } finally {
      if (in.ne(null))  {
        Try { in.close() }
        Try { conn.disconnect() }
      }
    }
  }

  // TODO-XXX: Try to replace with guava cache or scala cache
  var RefreshTime: Long = 0L
  var WebSecToken: String = _

  private def obtainWebSecToken(url: String, clientId: String, secret: String, scope: String, clearCache: Boolean = true): HttpResponse Either String = {
    if (clearCache) RefreshTime = 0L

    val nowInSeconds = new Date().getTime / 1000

    if (nowInSeconds < RefreshTime) return Right(WebSecToken)

    val headers: List[(String,String)] = List("Content-Type"->"application/x-www-form-urlencoded")
    val payload = s"client_id=${clientId}&client_secret=${secret}&scope=${scope}&grant_type=client_credentials"

    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats

    val resp = HttpUtil.post(url, headers, payload)
    if (resp.isSuccess()) {
      val respBody = resp.payload
      val webSecResponse = parse(respBody).extract[WebSecResponse]
      RefreshTime = nowInSeconds + (webSecResponse.`expires_in` - 60)
      WebSecToken = s"${webSecResponse.`token_type`} ${webSecResponse.`access_token`}"
      Right(WebSecToken)
    } else Left(resp)

  }

  case class WebSecResponse(
    `access_token`: String,
    `token_type`: String,
    `expires_in`: Long // Integer. The number of seconds the access token is valid.
  )


}