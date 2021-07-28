package poc.peaceland.DataProcessor.AnalysisProcessor

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, element_at, lit, to_date}
import poc.peaceland.Commons.utils.HdfsUtils._


case class DataPipeline(fs: FileSystem, spark: SparkSession)(implicit sparkSession: SparkSession ) {


  def getDataFromDirectpry(FILES_DIR: String, SOURCE_FILE_FORMAT: String): DataFrame = {

    val filesToLoad: List[String] = listFiles(fs, FILES_DIR).toList
    spark.read.format(SOURCE_FILE_FORMAT).load(filesToLoad: _*)
  }

  def countAlertsByDate(reports: DataFrame): DataFrame = {
    reports.withColumn("anneemoisjour", to_date(col("date"), "yyyy-MM-dd"))
    .groupBy("anneemoisjour").count
  }

  def countAlertsByPeaceWatcher(reports: DataFrame): DataFrame = {
    val indes = 1
    //reports.withColumn("peace_score", col("surrounding")(1))
    val report_coputed  = reports.withColumn("peace_score", col("surrounding")("_2"))
      .withColumn("anneemoisjour", to_date(col("date"), "yyyy-MM-dd"))
          .withColumn("limite", lit(50))
          .filter(col("peace_score") > 50)
          .drop("limite")

    report_coputed.groupBy(col("anneemoisjour")).agg(count("peace_score").as("Number alerts"))

  }

}
