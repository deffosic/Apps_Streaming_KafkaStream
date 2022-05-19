package processors.stateless

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import schema.Facture
import serdes.{JSONDeserializer, JSONSerializer}

import java.util.Properties

object WriteProcessors extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  implicit val jsonSerdes : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(String, jsonSerdes)
  implicit val producer : Produced[String, Facture] = Produced.`with`(String, jsonSerdes)

  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"action-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  val str : StreamsBuilder = new StreamsBuilder
  val kStrFacture : KStream[String, Facture] = str.stream[String, Facture]("factureJson")


  // Traitement
  val kstrTotal : KStream[String, Double] = kStrFacture.mapValues(f => f.orderLine.numUnits * f.orderLine.unitPrice)

  // selectKey
  val newKeys = kstrTotal.selectKey((k,v) => k.toUpperCase())
  newKeys.print(Printed.toSysOut().withLabel("Select keys"))

  // to() final processor
  newKeys.to("topic-test")(Produced.`with`(String, Double))

  // through() no final processor
  val t = newKeys.through("topic-test")(Produced.`with`(String, Double))

  // Transformation du KStream en KTable
  val kTable = str.table[String, Facture]("facturejson")(Consumed.`with`(String, jsonSerdes))


  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }

}
