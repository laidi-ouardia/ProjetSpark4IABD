package poc.peaceland.Commons.serializer

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import org.apache.kafka.common.serialization.Serializer
import poc.peaceland.Commons.schema.PeaceWatcherReport


class PeaceWatcherReportSerializer extends Serializer[PeaceWatcherReport]{

    override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

    }


    override def serialize(topic:String, data: PeaceWatcherReport):Array[Byte] = {
        try {
            val byteOut = new ByteArrayOutputStream()
            val objOut = new ObjectOutputStream(byteOut)
            objOut.writeObject(data)
            objOut.close()
            byteOut.close()
            byteOut.toByteArray
        }
        catch {
            case ex:Exception => throw new Exception(ex.getMessage)
        }
    }

    override def close():Unit = {

    }


}
