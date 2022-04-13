package serdes

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import org.apache.kafka.common.serialization.Serializer

class BytesSerializer[T] extends Serializer[T]{

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {

  }

  override def serialize(s: String, t: T): Array[Byte] = {
    if (t == null){
      return null
    } else {
      try {
        // c'est ici qu'est définie le ligne de codes pour sérialiser
        val bts = new ByteArrayOutputStream()
        val ost = new ObjectOutputStream(bts)

        ost.writeObject(t)
        ost.close()

        bts.toByteArray

      } catch {
        case e : Exception => throw new Exception(s"Erreur dans la sérialisation de ${t.getClass.getName} \n le détails est : ${e}")
      }
    }
  }

  override def close(): Unit = {
    ???
  }

}