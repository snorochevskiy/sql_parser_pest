package snorochevskiy.jobs

import snorochevskiy.config.ConfigProvider
import snorochevskiy.flow.{FederatedQueryExecutor, QueryInstruction}
import snorochevskiy.spark.SparkProvider


object CliSqlExecutionJob extends SparkProvider {

  def main(args: Array[String]): Unit = {
    val instruction = parseArgs(args)

    implicit val cfg = ConfigProvider.loadAppConf()

    FederatedQueryExecutor.execInstruction(instruction)
  }

  def parseArgs(args: Array[String]): QueryInstruction = {
    val kvRe = "(.*)=(.*)".r

    def inner(accum: QueryInstruction, args: List[String]): QueryInstruction = {
      args match {
        case Nil =>
          accum
        case kvRe("--input", input) :: rest =>
          inner(accum.copy(input = input), rest)
        case kvRe("--output", output) :: rest =>
          inner(accum.copy(output = output), rest)
        case kvRe("--format", outputFormat) :: rest =>
          inner(accum.copy(format = outputFormat), rest)
      }
    }

    inner(QueryInstruction(null, null, "parquet"), args.toList)
  }

}
