package serdes

import com.fasterxml.jackson.annotation.JsonInclude
import org.apache.kafka.common.serialization.Serializer
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.util

class JSONSerializer[T] extends Serializer[T]{

  val objetMapper : ObjectMapper = new ObjectMapper()
  objetMapper.registerModule(DefaultScalaModule)
  objetMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  objetMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
  objetMapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {

  }

  override def serialize(s: String, t: T): Array[Byte] = {
    if (t == null){
      return null
    } else {
      try {
        // c'est ici qu'est définie le ligne de codes pour sérialiser
        objetMapper.writeValueAsBytes(t)

      } catch {
        case e : Exception => throw new Exception(s"Erreur dans la sérialisation de ${t.getClass.getName} \n le détails est : ${e}")
      }
    }
  }

  override def close(): Unit = {
    ???
  }

}
