package poc.peaceland.Commons.schema

import java.sql.Timestamp

case class PeaceWatcherReport(
                                  lat: Double,
                                  lng: Double,
                                  date: Timestamp,
                                  peacewatcher_id: String,
                                  surrounding : (String,Int),
                                  words: List[String]
                                )
