package snorochevskiy.web.servlet

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import snorochevskiy.config.AppConf
import snorochevskiy.flow.{DirectQuery, FederatedQueryExecutor, QueryInstruction}
import snorochevskiy.io.InputFetcher

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, LinkedBlockingQueue}
import java.util.stream.Collectors
import javax.servlet.ServletConfig
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import scala.beans.BeanProperty

case class QueueExecElement(
  @BeanProperty var uuid: UUID,
  @BeanProperty var directQuery: DirectQuery
)

case class CompletedQuery(
  query: QueueExecElement,
  successful: Boolean,
  errors: Seq[String]
)

case class AllStatusResp(
  inQueue: Seq[QueueExecElement],
  currentlyProcessing: QueueExecElement,
  completed: Seq[CompletedQuery]
)

class SqlExecHttpServlet(spark: SparkSession, cfg: AppConf) extends HttpServlet {

  case class EnqueuedResp(@BeanProperty var UUID: UUID)

  private val jsonMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  private val queue = new LinkedBlockingQueue[QueueExecElement](10)
  private var currentlyProcessing: QueueExecElement = null
  private val completedQueries = new ConcurrentLinkedQueue[CompletedQuery]()

  override def init(config: ServletConfig): Unit = {
    super.init(config)

    val executor = Executors.newSingleThreadExecutor()
    executor.submit(new Runnable {
      override def run(): Unit = {
        val current@QueueExecElement(uuid, directQuery) = queue.take()
        currentlyProcessing = current
        println("Processing: " + current)
        val DirectQuery(queryDescription, output, format) = directQuery

        try
          FederatedQueryExecutor.exec(queryDescription, output, format)(spark, cfg) match {
            case Right(()) =>
              println("Successfully finished: " + current)
              completedQueries.add(CompletedQuery(current, true, Seq.empty))
            case Left(errors) =>
              println("Failed: " + current + "; errors: " + errors)
              completedQueries.add(CompletedQuery(current, false, errors))
          }
        catch {
          // TODO: check is spark session is alive and recreate if needed
          case e: Exception => e.printStackTrace()
        }

        currentlyProcessing = null
      }
    })
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    import collection.JavaConverters._
    val obj = AllStatusResp(
      inQueue = queue.stream().collect(Collectors.toList[QueueExecElement]).asScala,
      currentlyProcessing = currentlyProcessing,
      completed = completedQueries.stream().collect(Collectors.toList[CompletedQuery]).asScala
    )
    resp.setContentType("application/json");
    resp.setStatus(HttpServletResponse.SC_OK)
    resp.getWriter().println(jsonMapper.writeValueAsString(obj))
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    val result = req.getRequestURI() match {
      case "/submitInstruction" =>
        serveSubmitQueryInstruction(req)

      case "/submitQueryDirectly" =>
        serveSubmitQueryDirectly(req)
      case _ =>
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
        return ()
    }

    result match {
      case Right(uuid) =>
        val enqueuedResp = EnqueuedResp(uuid)
        resp.setContentType("application/json")
        resp.setStatus(HttpServletResponse.SC_OK)
        resp.getWriter().println(jsonMapper.writeValueAsString(enqueuedResp))
      case Left((code, msg)) =>
        resp.setContentType("application/json")
        resp.setStatus(code)
        resp.getWriter().println(msg)
    }
  }

  def serveSubmitQueryDirectly(req: HttpServletRequest): Either[(Int, String), UUID] = {
    val payloadTest = IOUtils.toString(req.getReader)
    val qi = try jsonMapper.readValue[DirectQuery](payloadTest, classOf[DirectQuery])
      catch {
        case e: Exception =>
          return Left(HttpServletResponse.SC_BAD_REQUEST, "Unable to deserialize payload: " + e.getMessage)
      }

    val element = QueueExecElement(UUID.randomUUID(), qi)
    if (queue.offer(element)) Right(element.uuid)
    else Left(429, "Queries queue is full")
  }

  def serveSubmitQueryInstruction(req: HttpServletRequest): Either[(Int, String), UUID] = {
    val payloadTest = IOUtils.toString(req.getReader)
    val qi = try jsonMapper.readValue[QueryInstruction](payloadTest, classOf[QueryInstruction])
      catch {
        case e: Exception =>
          return Left(HttpServletResponse.SC_BAD_REQUEST, "Unable to deserialize payload: " + e.getMessage)
      }

    val queryDescription = try InputFetcher.fetchJobInput(qi.input)
      catch {
        case _: IllegalArgumentException => return Left(HttpServletResponse.SC_BAD_REQUEST, "Bad URI: " + qi.input)
        case e: Exception => return Left(HttpServletResponse.SC_BAD_REQUEST, "Unable to deserialize query: " + e.getMessage)
      }

    val directQuery = DirectQuery(queryDescription = queryDescription, output = qi.output, format = qi.format)

    val element = QueueExecElement(UUID.randomUUID(), directQuery)

    if (queue.offer(element)) Right(element.uuid)
    else Left(429, "Queries queue is full")
  }

}
