package poc.peaceland.PeaceWatcherReportProducer

import java.sql.Timestamp
import play.api.libs.json._
import DataGenerator._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import poc.peaceland.Commons.AppConfig
//import poc.peaceland.Commons.JsonParser.DroneImageParser._
import poc.peaceland.Commons.JsonParser.PeaceWatcherReportParser._
import poc.peaceland.Commons.schema.PeaceWatcherReport
//import poc.peaceland.PeaceWatcherReportProducer.DataReportGenerator._


import scala.util.Random

class DataReportGenerator(producer: KafkaProducer[String, String])  extends AppConfig {
  import DataReportGenerator._
  def run() : Unit ={
    generateRandomPeaceWatcherReport()

  }

  private[this] def generateRandomNormalReport(): PeaceWatcherReport ={
    val lat :Double = latGenerator()
    val lng: Double = lngGenerator()
    val date: Timestamp = dateGenerator()
    val peacewatcher_id: String = droneIDGenerator()
    val surrounders: (String, Int) = surroundingGenerator(50, 100)
    val words = wordGenerator(false)
    PeaceWatcherReport(lat,lng,date,peacewatcher_id, surrounders, words)
  }

  private[this] def generateRandomImage(): DroneImage ={
    val image_id : String = imageIDGenerator()
    DroneImage(image_id)
  }

  private def generateRandomAlertReport() : PeaceWatcherReport = {
    val lat :Double =  latGenerator()
    val lng: Double = lngGenerator()
    val date: Timestamp = dateGenerator()
    val peacewatcher_id: String = droneIDGenerator()
    val surrounders: (String, Int) = surroundingGenerator(0, 49)
    val words = wordGenerator(true)
    PeaceWatcherReport(lat,lng,date,peacewatcher_id, surrounders, words)

  }
  @scala.annotation.tailrec
  private[this] def generateRandomPeaceWatcherReport(): Unit ={
    val rand: Int = 1 + Random.nextInt(20)
    if (rand == 19) {
      val report: PeaceWatcherReport = generateRandomAlertReport()
      val parsedAlertReport: String  = jsonToString(Json.toJson(report))
      producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_ALERT_KEY, parsedAlertReport))

    }
    else {
      val parsedNormalReport: String = jsonToString(Json.toJson(generateRandomNormalReport()))
      producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_NORMAL_KEY, parsedNormalReport))
    }
    Thread.sleep(1000)
    generateRandomPeaceWatcherReport()
  }

  private[this] def jsonToString(content: JsValue): String ={
    String.valueOf(content)
  }
}

object DataReportGenerator extends AppConfig{
  def apply(producer: KafkaProducer[String, String]): DataReportGenerator = new DataReportGenerator(producer)
  private val KAFKA_TOPIC: String = conf.getString("kafka.kafka_topic")
  private val KAFKA_NORMAL_KEY: String = conf.getString("kafka.kafka_normal_key")
  private val KAFKA_ALERT_KEY: String = conf.getString("kafka.kafka_alert_key")

}