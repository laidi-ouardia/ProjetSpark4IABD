package poc.peaceland.ConsumerReport

import org.apache.commons.mail.DefaultAuthenticator
import org.apache.commons.mail.SimpleEmail
import poc.peaceland.Commons.AppConfig
import poc.peaceland.Commons.schema.PeaceWatcherReport
import poc.peaceland.Commons.schema.PeaceWatcherReport
import poc.peaceland.Commons.utils.DateUtils.convertTimestampToString

object SendAlert extends AppConfig {

  private val MAIL_HOST = "smtp.mail.yahoo.com"
  private val MAIL_PORT = 465

  private val MAIL_ADRESSE = conf.getString("consumer_message.mail.mail_adress")
  private val MAIL_USERNAME = conf.getString("consumer_message.mail.user_name")
  private val MAIL_PASSWORD = conf.getString("consumer_message.mail.password")
  private val MAIL_SEND_TO = conf.getString("consumer_message.mail.mail_adress_send_to")


  def mailSender(violationMessage: PeaceWatcherReport) = {

    val messageText: String = convertMessageToMail(violationMessage)
    val subject: String = s"Peacewatcher Alert score !!!!!"

    val email = new SimpleEmail()
    email.setHostName(MAIL_HOST)
    email.setSmtpPort(MAIL_PORT)
    email.setAuthenticator(new DefaultAuthenticator(MAIL_USERNAME, MAIL_PASSWORD))
    email.setSSLOnConnect(true)
    email.setFrom(MAIL_ADRESSE)
    email.setSubject(subject)
    email.setMsg(messageText)
    email.addTo(MAIL_SEND_TO)
    email.send
  }

  private[this] def convertMessageToMail(alert: PeaceWatcherReport): String = {
    val peaceScore: String= alert.surrounding._2.toString
    val personName: String= alert.surrounding._1
    val alertDate = convertTimestampToString(alert.date)
    val alertLat = alert.lat.toString
    val alertLng = alert.lng.toString



    s"""|New ALERT on citizen's hapiness
        |Peace Score : $peaceScore
        |Citizen name : $personName
        |Date: $alertDate
        |Latitude : $alertLat
        |Longitude : $alertLng
        |
        |
        |-- SENT FROM ALERT HANDLER SYSTEM --
        |""".stripMargin
  }

}

