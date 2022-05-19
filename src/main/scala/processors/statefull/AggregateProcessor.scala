package processors.statefull

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, Materialized, Produced}
import schema.{Facture, OrderLine}
import serdes.{JSONDeserializer, JSONSerializer}

import java.util.Properties

object AggregateProcessor extends  App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  implicit val jsonSerdes : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val producer : Produced[String, Facture] = Produced.`with`(Serdes.String(), jsonSerdes)

  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"aggregate-processor-01")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  val str : StreamsBuilder = new StreamsBuilder
  val kStrFacture : KStream[String, Facture] = str.stream[String, Facture]("facturejson")

  val kCA1 = kStrFacture
    .map((_,f) => ("1", f.total))
    .groupBy((k,t) => k)(Grouped.`with`(String, Double))
    .aggregate(0D)((k, newValue, aggValue) => (aggValue + newValue))

  kCA1.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global 1"))

  //methode 2

  val kCA2 = kStrFacture
    .map((_,f) => ("1", f))
    .groupBy((k,t) => k)
    .aggregate(0D)((k, newFacture, aggFacture) => (aggFacture + newFacture.total))

  kCA2.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global 2"))

  // Calcul du chiffre d'affaire moyen
  val kCA3 = kStrFacture
    .map((_,f) => ("1", f))
    .groupBy((k,t) => k)
    .aggregate[Facture](Facture("", "", 0, 0D, OrderLine("", "", "", "", 0D, 0D, 0)))(
      (k, newFacture, aggFacture) =>  Facture(newFacture.factureId, "", newFacture.quantite + aggFacture.quantite, newFacture.total + aggFacture.total, OrderLine("", "", "", "", 0D, 0D, 0))
    ).mapValues(f => (f.quantite, f.total, f.total/f.quantite))
  kCA3.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire moyen 3"))

  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }

}
