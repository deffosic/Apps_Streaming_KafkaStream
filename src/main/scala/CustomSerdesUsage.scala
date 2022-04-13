import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import schema.{Facture, OrderLine}
import serdes.{BytesDeserializer, BytesSerDes, BytesSerializer, JSONDeserializer, JSONSerializer}
import HelloWordKafkaStreams.getParams


object CustomSerdesUsage extends App {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes.String
  import org.apache.kafka.streams.scala.Serdes.Integer

  // Première utilisation d'un serdes

  //En bytes
  implicit val bytesSerder = new BytesSerDes[Facture]
  implicit val bytesOrderSerdes = new BytesSerDes[OrderLine]
  //En Json
  //implicit val bytesSerder1 = new JSONSerDes[Facture]
  //implicit val bytesOrderSerdes1 = new JSONSerDes[OrderLine]

  // Deuxième utilisation d'un serdes
  // En JSON
  implicit val factureSerdesJSON : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture] , new JSONDeserializer)
  // En Byte
  implicit val factureSerdesBYTE : Serde[Facture] = Serdes.serdeFrom[Facture](new BytesSerializer[Facture] , new BytesDeserializer[Facture])

  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), factureSerdesJSON)
  implicit val produced : Produced[String, Facture] = Produced.`with`(Serdes.String(), factureSerdesJSON)


  val str : StreamsBuilder = new StreamsBuilder
  val kstr : KStream[String, Facture] = str.stream[String, Facture]("streams_app")
  val kstrMaj : KStream[String, Int] = kstr.mapValues(v => v.quantite)
  kstrMaj.to("streams_app_upper")

  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, getParams("custumapp","locahost:9092"))
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }

}