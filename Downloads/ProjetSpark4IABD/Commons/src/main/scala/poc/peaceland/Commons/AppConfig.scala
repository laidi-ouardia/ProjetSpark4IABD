package poc.peaceland.Commons

import com.typesafe.config.{Config, ConfigFactory}

abstract class AppConfig {

  val conf: Config = ConfigFactory.load()

}