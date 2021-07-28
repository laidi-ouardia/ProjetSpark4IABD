package poc.peaceland.PeaceWatcherReportProducer

import java.sql.Timestamp

import scala.util.Random

object DataGenerator {

  def latGenerator(): Double = {
    val lat: Int = -83
    lat - Random.nextDouble()
  }

  def optLatGenerator(): Option[Double] = {
    Some(latGenerator())
  }


  def lngGenerator(): Double = {
    val lng: Int = 30
    lng + Random.nextDouble()
  }

  def optLngGenerator(): Option[Double] = {
    Some(lngGenerator())
  }

  def dateGenerator(): Timestamp = {
    new Timestamp(System.currentTimeMillis())
  }

  def optDateGenerator(): Option[Timestamp] = {
    Some(dateGenerator())
  }

  def droneIDGenerator(): String = {
    val start: Int = 1
    val end: Int = 9999999
    String.valueOf(start + Random.nextInt(end - start))
  }

  def optDroneIDGenerator(): Option[String] = {
    Some(droneIDGenerator())
  }

  def surroundingGenerator(start: Int, end: Int): (String, Int) = {
    val rnd = new scala.util.Random
    val scoreIndex = start + rnd.nextInt((end - start) + 1)
    val nameIndex = Random.nextInt(10)
    val names: List[String] = List("Lynda", "Ouardia", "Jim Carrey", "Bill Gates", "Bob Marley", "Kelyan Mbappé", "Mr Bean", "Jackie Chan", "Dalai Lama", "Matoub Lounes")
    (names(nameIndex) -> scoreIndex)
  }

  def wordGenerator(alert: Boolean): List[String] = {
    val wordIndex = List.fill(3)(Random.nextInt(5))
    if (alert) {
      wordIndex.map(List("Revolution", "Wrights", "Burn", "Freedom", "Violence")(_))
    }
    else {
      wordIndex.map(List("Hello", "Car", "Today", "Man", "Home", "Money", "Children")(_))
    }
  }

}
