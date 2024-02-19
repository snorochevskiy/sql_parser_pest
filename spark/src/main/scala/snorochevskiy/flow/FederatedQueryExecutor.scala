package snorochevskiy.flow

import org.apache.spark.sql.{SaveMode, SparkSession}
import snorochevskiy.config.{AppConf, DatabaseConf}
import snorochevskiy.io.InputFetcher
import snorochevskiy.jobs.QueryInstruction

object FederatedQueryExecutor {

  def exec(instruction: QueryInstruction)(implicit spark: SparkSession, cfg: AppConf): Unit = {
    val jobInput = InputFetcher.fetchJobInput(instruction.input)

    for (withStmt <- jobInput.withBlocks) {
      fromJdbc(cfg.findDb(withStmt.dbName).get, withStmt.sql, withStmt.name)
    }

    spark.catalog.setCurrentDatabase(cfg.useCatalog.name)
    val result = spark.sql(jobInput.finalSql)

    result.write
      .format(instruction.format)
      .mode(SaveMode.Append)
      .save(instruction.output)
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
}
