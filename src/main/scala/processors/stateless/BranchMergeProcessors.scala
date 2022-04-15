package processors.stateless

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import schema.Facture
import serdes.{JSONDeserializer, JSONSerializer}

import java.util.Properties

object BranchMergeProcessors extends App {

  implicit val jsonSerdes : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val producer : Produced[String, Facture] = Produced.`with`(Serdes.String(), jsonSerdes)

  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"action-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  val str : StreamsBuilder = new StreamsBuilder
  val kStrFacture : KStream[String, Facture] = str.stream[String, Facture]("factureJson")


  // Traitement
  val kstrTotal : KStream[String, Double] = kStrFacture.mapValues(f => f.orderLine.numUnits * f.orderLine.unitPrice)
  val kstrBranch : Array[KStream[String, Double]] = kstrTotal.branch(
    (_, t) => t>10000,
    (_, t) => t > 5000,
    (_, t) => t > 2000,
    (_, t) => t <= 2000
  )

  val kstr2K = kstrBranch(3)
  kstr2K.foreach{
    (k,m) => println(s"Facture inférieur à 2k : ${k} => ${m}")
  }
  //methode 2
  val liste_predicats : List[(String, Double) => Boolean] = List(
    (_, t) => t>10000,
    (_, t) => t > 5000,
    (_, t) => t > 2000,
    (_, t) => t <= 2000
  )
  val kstrBranch2 : Array[KStream[String, Double]] = kstrTotal.branch(liste_predicats:_*)
  val kstr10k = kstrBranch2(0)

  // merge
  val kMerge = kstr2K.merge(kstr10k.merge(kstrBranch(1)))


  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }

}
