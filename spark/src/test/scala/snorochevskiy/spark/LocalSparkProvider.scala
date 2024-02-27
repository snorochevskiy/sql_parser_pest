package snorochevskiy.spark

import org.apache.spark.sql.SparkSession

trait LocalSparkProvider extends SparkProvider {

  implicit override lazy val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.ui.enabled", false)
    .appName("LocalSpark")
    .getOrCreate()
}