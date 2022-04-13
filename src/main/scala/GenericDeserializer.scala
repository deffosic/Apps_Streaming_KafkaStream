package serdes

import org.apache.kafka.common.serialization.Deserializer

import java.util

class GenericDeserializer[T] extends Deserializer[T] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ???

  override def deserialize(s: String, bytes: Array[Byte]): T = ???

  override def close(): Unit = ???
}
