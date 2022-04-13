package serdes
import org.apache.kafka.common.serialization.Serializer

import java.util

class GenericSerializer [T] extends Serializer[T] {
  val arraybyte = Array.emptyByteArray

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {

  }

  override def serialize(s: String, t: T): Array[Byte] = {
    if (t == null){
      return null
    } else {
      try {
        // c'est ici qu'est définie le ligne de codes pour sérialiser
        arraybyte
      } catch {
        case e : Exception => throw new Exception(s"Erreur dans la sérialisation de ${t.getClass.getName} \n le détails est : ${e}")
      }
    }
  }

  override def close(): Unit = {
    ???
  }
}