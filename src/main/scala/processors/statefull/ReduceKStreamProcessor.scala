package processors.statefull

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, Materialized, Produced}
import schema.Facture
import serdes.{JSONDeserializer, JSONSerializer}

import java.util.Properties

object ReduceKStreamProcessor extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  implicit val jsonSerdes : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val producer : Produced[String, Facture] = Produced.`with`(Serdes.String(), jsonSerdes)

  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"reduce-processor-01")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  val str : StreamsBuilder = new StreamsBuilder
  val kStrFacture : KStream[String, Facture] = str.stream[String, Facture]("facturejson")

  val kCA = kStrFacture
    .map((_,f) => ("1", f.total))
    .groupBy((k,t) => k)(Grouped.`with`(String, Double))
    .reduce((aggValue, currentValue) => aggValue + currentValue)(Materialized.as("reduceStore"))

  kCA.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global"))

  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }

}
