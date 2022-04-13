package serdes
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util
import org.apache.kafka.common.serialization.Deserializer
class BytesDeserializer[T] extends Deserializer[T]{


  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
    ???
  }

  override def deserialize(s: String, bytes: Array[Byte]): T = {
    //bytes match {
    //case null => null
    //case _ =>
    try {
      val btos = new ByteArrayInputStream(bytes)
      val oist = new ObjectInputStream(btos)

      oist.readObject().asInstanceOf[T]
    } catch {
      case e : Exception => throw new Exception(s"Erreur dans la désérialisation du message ${bytes}. \n le détails est : ${e}")
    }
    // }
  }

  override def close(): Unit = {
    ???
  }
}