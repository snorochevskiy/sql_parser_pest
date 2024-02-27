package snorochevskiy.flow

import org.apache.spark.sql.{SaveMode, SparkSession}
import snorochevskiy.config.{AppConf, DatabaseConf}
import snorochevskiy.io.{InputFetcher, JobInput}
import snorochevskiy.util.JdbcUtil

import scala.beans.BeanProperty

case class QueryInstruction(
  @BeanProperty var input: String,
  @BeanProperty var output: String,
  @BeanProperty var format: String,
) {
  def this() = this(null,null,null)
}

case class DirectQuery(
  @BeanProperty var queryDescription: JobInput,
  @BeanProperty var output: String,
  @BeanProperty var format: String,
) {
  def this() = this(null,null,null)
}

object FederatedQueryExecutor {

  def execInstruction(instruction: QueryInstruction)(implicit spark: SparkSession, cfg: AppConf) = {
    val jobInput = InputFetcher.fetchJobInput(instruction.input)

    exec(jobInput, instruction.output, instruction.format)
  }

  def exec(jobInput: JobInput, output: String, format: String)(implicit spark: SparkSession, cfg: AppConf): Either[Seq[String],Unit] = {

    // Validating queries to RDBMSs
    val rdbmsSqlErrs = jobInput.withBlocks
      .map(withStmt => cfg.findDb(withStmt.dbName).toRight(s"No database configured for: ${withStmt.dbName}")
        .flatMap { case DatabaseConf(_, url, user, passwd) => JdbcUtil.validateRdbmsQuery(url, user, passwd, withStmt.sql) }
      )
      .foldLeft(Nil: List[String]) { (acc, e) =>
        e match {
          case Right(()) => acc
          case Left(err) => err :: acc
        }
      }
    if (rdbmsSqlErrs.nonEmpty) return Left(rdbmsSqlErrs)

    // Preparing data from RDBMs as data frames
    for (withStmt <- jobInput.withBlocks)
      fromJdbc(cfg.findDb(withStmt.dbName).get, withStmt.sql, withStmt.withViewName)

    try {
      spark.catalog.setCurrentDatabase(cfg.useCatalog.name)
      val datalakeSqlErr = validateDatalakeQuery(jobInput.finalSql)
      if (datalakeSqlErr.isLeft) return Left(Seq(datalakeSqlErr.left.get))

      val result = spark.sql(jobInput.finalSql)

      result.write
        .format(format)
        .mode(SaveMode.Append)
        .save(output)
    } finally {
      for (withStmt <- jobInput.withBlocks)
        spark.catalog.dropTempView(withStmt.withViewName)
    }
    Right(())
  }

  def fromJdbc(dbConf: DatabaseConf, query: String, targetTable: String)
              (implicit spark: SparkSession) = {
    val df = spark.read.format("jdbc")
      .option("url", dbConf.connectionUrl)
      .option("user", dbConf.user)
      .option("password", dbConf.password)
      .option("query", query)
      .load()

    df.createOrReplaceTempView(targetTable)
  }

  def validateDatalakeQuery(query: String)(implicit spark: SparkSession): Either[String, Unit] =
    try {
      spark.sql(s"DESCRIBE ${query}")
      Right(())
    } catch {
      case e: Exception => Left("Resulting SQL query validation error: " + e.getMessage)
    }
}
