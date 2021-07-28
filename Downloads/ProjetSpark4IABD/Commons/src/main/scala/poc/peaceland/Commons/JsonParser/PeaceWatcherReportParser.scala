package poc.peaceland.Commons.JsonParser

import java.sql.Timestamp
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import poc.peaceland.Commons.schema.PeaceWatcherReport

object PeaceWatcherReportParser {
  def timestampToLong(t: Timestamp): Long = t.getTime
  def longToTimestamp(dt: Long): Timestamp = new Timestamp(dt)

  implicit val timestampFormat: Format[Timestamp] = new Format[Timestamp] {
    def writes(t: Timestamp): JsValue = Json.toJson(timestampToLong(t))
    def reads(json: JsValue): JsResult[Timestamp] = Json.fromJson[Long](json).map(longToTimestamp)
  }

  implicit val droneStandarMessageReads: Reads[PeaceWatcherReport] = (
    (JsPath \ "lat").read[Double] and
      (JsPath \ "lng").read[Double] and
      (JsPath \ "date").read[Timestamp] and
      (JsPath \ "peacewatcher_id").read[String] and
      (JsPath \ "surrounding").read[(String, Int)] and
      (JsPath \ "words").read[List[String]]

    )(PeaceWatcherReport.apply _)

  implicit val droneStandarMessageWrites: Writes[PeaceWatcherReport] = (
    (JsPath \ "lat").write[Double] and
      (JsPath \ "lng").write[Double] and
      (JsPath \ "date").write[Timestamp] and
      (JsPath \ "peacewatcher_id").write[String] and
      (JsPath \ "surrounding").write[(String, Int)] and
      (JsPath \ "words").write[List[String]]
    ) (unlift(PeaceWatcherReport.unapply))

}
