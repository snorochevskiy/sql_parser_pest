package snorochevskiy.io

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import snorochevskiy.util.Bracket

import java.net.URI
import java.nio.charset.Charset
import scala.io.Source


case class JobInput(
  withBlocks: Seq[WithBlock],
  finalSql: String,
)

case class WithBlock(dbName: String, name: String, sql: String)

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
      throw new IllegalAccessException("Not supported URL type")
    }

    parseJobInput(text)
  }

  def parseJobInput(xmlText: String): JobInput = {
    import scala.language.postfixOps

    val xml = scala.xml.XML.loadString(xmlText)
    val withBlocks = xml \\ "queries" \\ "with-queries" map { node =>
      WithBlock(dbName = node \\ "dbName" text, name = node \\ "name" text, sql = node \\ "sql"  text)
    }
    val finalQuery = (xml \\ "queries" \\ "final-query" \\ "sql").head.text

    JobInput(withBlocks, finalQuery)
  }

}
