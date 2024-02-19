package snorochevskiy.spark

import org.apache.spark.sql.SparkSession

trait SparkProvider {

  implicit lazy val spark = SingletonSparkProvider.spark

}

object SingletonSparkProvider {
  lazy val spark = SparkSession.builder
    .appName("SqlExecutionJob")
    .enableHiveSupport()
    .getOrCreate()

}