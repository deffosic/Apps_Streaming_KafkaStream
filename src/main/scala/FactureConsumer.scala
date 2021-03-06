import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import schema.Facture

import scala.collection.JavaConverters._
import serdes.JSONDeserializer

import java.time.Duration
import java.util.{Collections, Properties}

object FactureConsumer extends App {
  val props = new Properties()
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[JSONDeserializer])
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupe_orders")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val factureConsumer = new KafkaConsumer[String, Facture](props)
  factureConsumer.subscribe(Collections.singletonList(""))

  while (true) {
    val messages : ConsumerRecords[String, Facture] = factureConsumer.poll(3)
    println(s"Le nombre de messages collectés dans la fenêtre : ${messages.count()}")
    if (!messages.isEmpty) {
      for(message <- messages.asScala) {
        println(" Topic : "+message.topic()+
          " Key : "+message.key()+
          " Value : "+message.value()
        )
      }
    }
  }


}
