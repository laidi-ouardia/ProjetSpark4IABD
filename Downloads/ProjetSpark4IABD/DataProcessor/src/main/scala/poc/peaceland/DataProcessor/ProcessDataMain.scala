package poc.peaceland.DataProcessor

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import poc.peaceland.Commons.AppConfig
import poc.peaceland.DataProcessor.AnalysisProcessor.DataPipeline



object ProcessDataMain extends AppConfig {

    def main(args: Array[String]): Unit = {

        val SPARKSESSION_APPNAME: String = conf.getString("spark.appname")
        val SPARKSESSION_MASTER: String = conf.getString("spark.master")
        val HDFS_FILE_PATH: String = conf.getString("hdfs.source_file.path")
        val HDFS_FILE_FORMAT: String = conf.getString("hdfs.source_file.format")
        val FILES_DIR: String = conf.getString("hdfs.source_file.normal_report")



        implicit val sparkSession: SparkSession =
            SparkSession.builder()
              .appName(SPARKSESSION_APPNAME)
              .master(SPARKSESSION_MASTER)
              .getOrCreate()
        val sparkConf  = sparkSession.sparkContext.hadoopConfiguration
        val fs: FileSystem = FileSystem.get(sparkConf)

        val pipeline: DataPipeline = new DataPipeline(fs, sparkSession)
        val data = pipeline.getDataFromDirectpry(FILES_DIR, HDFS_FILE_FORMAT)
        data.show(false)

        pipeline.countAlertsByPeaceWatcher(data).show(false)
        pipeline.countAlertsByDate(data).show(false)


        sparkSession.stop()
    }
}
