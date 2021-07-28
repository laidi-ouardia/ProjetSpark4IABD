package poc.peaceland.Commons.deserializer

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util
import org.apache.kafka.common.serialization.Deserializer
import poc.peaceland.Commons.schema.PeaceWatcherReport

class PeaceWatcherReportDeserializer extends Deserializer[PeaceWatcherReport]{

    override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

    }
    override def deserialize(topic:String,bytes: Array[Byte]): PeaceWatcherReport = {
        val byteIn = new ByteArrayInputStream(bytes)
        val objIn = new ObjectInputStream(byteIn)
        val obj = objIn.readObject().asInstanceOf[PeaceWatcherReport]
        byteIn.close()
        objIn.close()
        obj
    }
    override def close():Unit = {

    }

}
