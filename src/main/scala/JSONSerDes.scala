package serdes

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import java.util

class JSONSerDes[T] extends Serde[T]{
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ???

  override def close(): Unit = ???

  override def serializer(): Serializer[T] = new JSONSerializer[T]

  override def deserializer(): Deserializer[T] = new JSONDeserializer[T]
}