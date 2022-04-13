package serdes

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Deserializer
import schema.Facture

import scala.reflect.{ClassTag, classTag}
import java.util

class JSONDeserializer extends Deserializer[Facture]{

  val objetMapper : ObjectMapper = new ObjectMapper()
  objetMapper.registerModule(DefaultScalaModule)
  objetMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  objetMapper.configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, true)
  objetMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)


  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
    ???
  }

  override def deserialize(s: String, bytes: Array[Byte]): Facture = {
    //bytes match {
    //case null => null
    //case _ =>
    try {
      objetMapper.readValue(bytes,  classOf[Facture])
    } catch {
      case e : Exception => throw new Exception(s"Erreur dans la désérialisation du message ${bytes}. \n le détails est : ${e}")
    }
    // }
  }

  override def close(): Unit = {
    ???
  }
}
