package snorochevskiy.io

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import snorochevskiy.util.Bracket

import java.net.URI
import java.nio.charset.Charset
import scala.beans.BeanProperty
import scala.io.Source


case class JobInput(
  @BeanProperty var withBlocks: Seq[WithBlock],
  @BeanProperty var finalSql: String,
) {
  def this() = this(null,null)
}

case class WithBlock(dbName: String, withViewName: String, sql: String)

object InputFetcher {

  def fetchJobInput(inputUrl: String): JobInput = {
    val text = if (inputUrl startsWith "file") {
      IOUtils.toString(new java.net.URL(inputUrl), Charset.defaultCharset())
    } else if (inputUrl startsWith "http") {
      IOUtils.toString(new java.net.URL(inputUrl), Charset.defaultCharset())
    } else if (inputUrl startsWith "s3") {
      val fs = FileSystem.get(URI.create(inputUrl), new Configuration())
      Bracket.withCloseable2(fs.open(new Path(inputUrl)))(Source.fromInputStream(_)) { source =>
        source.mkString
      }
    } else {
      throw new IllegalArgumentException("Not supported URL type")
    }

    parseQuery(text)
  }

  def parseQuery(xmlText: String): JobInput = {
    import scala.language.postfixOps

    val xml = scala.xml.XML.loadString(xmlText)
    val withBlocks = xml \\ "queries" \\ "withQueries" map { node =>
      WithBlock(dbName = node \\ "dbName" text, withViewName = node \\ "withViewName" text, sql = node \\ "sql"  text)
    }
    val finalQuery = (xml \\ "queries" \\ "finalQuery" \\ "sql").head.text

    JobInput(withBlocks, finalQuery)
  }

}
