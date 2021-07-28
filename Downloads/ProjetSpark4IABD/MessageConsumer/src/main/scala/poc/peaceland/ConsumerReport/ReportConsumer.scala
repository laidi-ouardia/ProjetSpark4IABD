package poc.peaceland.ConsumerReport

import java.time.Duration
import SendAlert._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import poc.peaceland.Commons.schema.{DroneImage, PeaceWatcherReport}
import poc.peaceland.Commons.JsonParser.PeaceWatcherReportParser._
import poc.peaceland.Commons.JsonParser.PeaceWatcherReportParser._
import poc.peaceland.Commons.JsonParser.DroneImageParser._
import poc.peaceland.Commons.utils.DateUtils._
import org.apache.spark.sql.SaveMode.Append

import scala.jdk.CollectionConverters._
import play.api.libs.json._
import poc.peaceland.Commons.AppConfig
import poc.peaceland.Commons.schema.PeaceWatcherReport
import poc.peaceland.Commons.schema.{DroneImage, PeaceWatcherReport}

import java.io._
import scala.util.Try

class ReportConsumer(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, String]) {
  import ReportConsumer._

  def run(): Unit = {
    startReadingMessages(Nil, Nil)
  }
  @scala.annotation.tailrec
  private[this] def startReadingMessages(previousNormalReport: List[PeaceWatcherReport], previousAlertReport: List[PeaceWatcherReport]): Unit = {

    if ((previousNormalReport != Nil) && previousNormalReport.length >= BATCH_SIZE_FOR_FILE_WRITING_WITH_SPARK) {
      println("INFO - save Standard and Violation messages  ")

      saveNormalBatch(previousNormalReport)
      saveAlertBatch(previousAlertReport)
      kafkaConsumer.commitAsync()
      println("SUCCESS - Normal and Alert messages Batch successfully saved - start new batch about to run")
      startReadingMessages(Nil, Nil)
    }
    else {
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMinutes(KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES))
      val recordsIterator: Iterator[ConsumerRecord[String, String]] = records.iterator().asScala
      val updatedLists: (List[PeaceWatcherReport], List[PeaceWatcherReport]) = manageMessages(previousNormalReport, previousAlertReport, recordsIterator)
      startReadingMessages(updatedLists._1, updatedLists._2)
    }

  }
  private[this] def manageMessages(previousNormalReport: List[PeaceWatcherReport],
                                   previousAlertReport: List[PeaceWatcherReport],
                                   iter: Iterator[ConsumerRecord[String, String]]): (List[PeaceWatcherReport], List[PeaceWatcherReport]) ={

    if(iter.hasNext) {

      val elem = iter.next()
      elem.key() match {

        case KAFKA_NORMAL_KEY =>
          println("INFO - Received Standard Message")
          val standardMessageJson:JsValue = Json.parse(elem.value())
          standardMessageJson.validate[PeaceWatcherReport] match {
            case s: JsSuccess[PeaceWatcherReport] =>
              val standardMessage: PeaceWatcherReport = s.get
              (addStandardRecordToBatch(previousNormalReport, standardMessage), previousAlertReport)
            case _: JsError =>
              print("ERROR - Lost standard drone message")
              (previousNormalReport, previousAlertReport)
          }


        case KAFKA_ALERT_KEY =>
          println("WARN - Received Violation Message")
          val violationMessageJson:JsValue=Json.parse(elem.value())
          violationMessageJson.validate[PeaceWatcherReport] match {
            case s: JsSuccess[PeaceWatcherReport] =>
              val violationMessage: PeaceWatcherReport = s.get
              mailSender(violationMessage)
              (previousNormalReport, addViolationRecordToBatch(previousAlertReport, violationMessage))
            case e: JsError =>
              print("error!")
              (previousNormalReport, previousAlertReport)
          }

        case _ =>
          printf("WARNING - Unhandled message key")
          (previousNormalReport, previousAlertReport)
      }
    } else {
      (previousNormalReport, previousAlertReport)
    }
  }

  private[this] def addStandardRecordToBatch(batch: List[PeaceWatcherReport], message: PeaceWatcherReport): List[PeaceWatcherReport] = {
    batch match {
      case Nil =>
        message :: Nil
      case _ =>
        batch ::: (message :: Nil)
    }
  }

  private[this] def addViolationRecordToBatch(batch: List[PeaceWatcherReport], message: PeaceWatcherReport): List[PeaceWatcherReport] = {
    batch match {
      case Nil =>
        message :: Nil
      case _ =>
        batch ::: (message :: Nil)
    }
  }

  private[this] def pathExists(path: String): Boolean = {
    val file: File = new File(path)
    file.exists()
  }



  private[this] def saveNormalBatch(fileBatchContent: List[PeaceWatcherReport]): Unit = {
    import spark.implicits._

    val batchDataframe: DataFrame =
      fileBatchContent.toDF()
        .repartition(NB_DEFAULT_SPARK_PARTITIONS)

    batchDataframe.write.mode(Append).format(WRITING_ALERT_FILE_FORMAT).save(filePathForStandardMessage)
  }

  private[this] def saveAlertBatch(fileBatchContent: List[PeaceWatcherReport]): Unit = {
    import spark.implicits._

    val batchDataframe: DataFrame =
      fileBatchContent.toDF()
        .repartition(NB_DEFAULT_SPARK_PARTITIONS)

    batchDataframe.write.mode(Append).format(WRITING_ALERT_FILE_FORMAT).save(filePathForViolationMessage)
  }


  private[this] val filePathForStandardMessage=s"$HDFS_NORMAL_TARGET_DIR/$TARGET_NORMAL_FILE_NAME"
  private[this] val filePathForViolationMessage=s"$HDFS_ALERT_TARGET_DIR/$TARGET_ALERT_FILE_NAME"


}

object ReportConsumer extends AppConfig {
  def apply(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, String]): ReportConsumer = new ReportConsumer(spark, kafkaConsumer)

  val KAFKA_ALERT_KEY=conf.getString("producer_message.kafka.kafka_alert_key")
  val KAFKA_NORMAL_KEY=conf.getString("producer_message.kafka.kafka_normal_key")

  val MAIN_KAFKA_TOPIC: String = conf.getString("producer_message.kafka.kafka_topic")
  val KAFKA_BOOTSTRAP_SERVERS: String = conf.getString("consumer_message.kafka.bootstrap_server")

  val KAFKA_MAIN_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_main_consumers_close_duration_minutes")
  val KAFKA_MAIN_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_main_consumers_poll_duration_minutes")
  val KAFKA_MAIN_CONSUMERS_GROUP_ID: String = conf.getString("consumer_message.kafka.consumers.kafka_main_consumers_group_id")

  val KAFKA_FILES_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_close_duration_minutes")
  val KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_poll_duration_minutes")
  val KAFKA_FILE_CONSUMERS_GROUP_ID_PREFIX: String = "-group"

  val BATCH_SIZE_FOR_FILE_WRITING_WITH_SPARK: Int = conf.getInt("consumer_message.kafka.consumers.spark_writing_batch_size")
  val HDFS_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.target_directory")

  val NB_DEFAULT_SPARK_PARTITIONS: Int = conf.getInt("consumer_message.spark.default_partitions")



  val WRITING_NORMAL_FILE_FORMAT: String = conf.getString("consumer_message.hdfs_files.normal_file_format")
  val WRITING_ALERT_FILE_FORMAT: String = conf.getString("consumer_message.hdfs_files.alert_file_format")
  val TARGET_NORMAL_FILE_NAME: String = conf.getString("consumer_message.hdfs_files.normal_file_name")
  val HDFS_NORMAL_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.normal_target_dir")
  val TARGET_ALERT_FILE_NAME: String = conf.getString("consumer_message.hdfs_files.alert_file_name")
  val HDFS_ALERT_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.alert_target_dir")


}
