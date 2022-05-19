import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import schema.Facture

import java.util.Properties

object KTableComputations extends  App{
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"ktable-test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  val str : StreamsBuilder = new StreamsBuilder
  val ktblTest : KTable[String, String] = str.table("ktabletest")

  // Création d'un State Store à l'initialisation d'un KTable
  val ktbl1 : KTable[String, String] = str.table("ktabletest1", Materialized.as("STATE-STORE-STR"))


  // Traitement
  ktblTest.toStream.print(Printed.toSysOut().withLabel("Clé/Valeur de KTable"))



  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }
}
