package processors.stateless

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import schema.Facture
import serdes.{JSONDeserializer, JSONSerializer}

import java.util
import java.util.Properties

object ActionsProcessors extends App {

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
  val kstrProducts : KStream[String, Array[String]] = kStrFacture.flatMapValues(f => List(f.productName.split(" ")))

  val kstrTotal : KStream[String, Double] = kStrFacture.flatMapValues(f => List(f.orderLine.numUnits * f.orderLine.unitPrice, f.orderLine.numUnits, f.orderLine.unitPrice))

  val kstrTotal2 : KStream[String, Double] = kStrFacture.flatMap(
    (k,f) => List(
      (k.toUpperCase(), f.orderLine.numUnits * f.orderLine.unitPrice),
      (k.substring(1,2), f.orderLine.numUnits),
      (k.substring(1),f.orderLine.unitPrice)
    )
  )
  kstrTotal2.peek{
    (k,v) => println(s"Clé du message ${k}, valeur du message ${v}")
  }

  //kstrTotal2.print(Printed.toFile("/monfichier.bat").withLabel("print"))
  kstrTotal2.print(Printed.toSysOut().withLabel("test"))

  //marque la fin du programme (topologie)
  kstrTotal2.foreach{
    (k,v) => println(s"Clé du message ${k}, valeur du message ${v}")
  }



  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }

}
