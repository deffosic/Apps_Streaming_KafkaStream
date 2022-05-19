package processors.statefull

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Printed, SessionWindows, TimeWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, Produced}
import schema.{Facture, OrderLine}
import serdes.{JSONDeserializer, JSONSerializer}

import java.time.{Duration, ZoneOffset}
import java.util.Properties

object WindowingProcess extends App {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._


  implicit val jsonSerdes : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture], new JSONDeserializer)
  //implicit val consumed : Consumed[String, Facture] = Consumed.`with`(String, jsonSerdes)
  implicit val consumedTimestamp : Consumed[String, Facture] = Consumed.`with`(new FactureTimestampExtractor)(String, jsonSerdes)
  implicit val producer : Produced[String, Facture] = Produced.`with`(String, jsonSerdes)

  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"windowing-process-02")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
  props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,"org.apache.kafka.streams.processor.WallclockTimestampExtractor") //temps d'ingestion au niveau global
  //props.put("message.timestamp.type", "CreateTime(temps de création)/LogAppendTime(temps d'ingestion")

  val str : StreamsBuilder = new StreamsBuilder
  val kStrFacture : KStream[String, Facture] = str.stream[String, Facture]("facturejson")(consumedTimestamp)

  // fenêtre fixe
  println("Fenêtre fixe")
  val kCA1 = kStrFacture
    .map((_,f) => ("1", f.total))
    .groupBy((k,t) => k)(Grouped.`with`(String, Double))
    .windowedBy(TimeWindows.of(Duration.ofSeconds(15)))
    //.windowedBy(TimeWindows.of(Duration.ofSeconds(15)).grace(Duration.ofSeconds(3)))  // delais de cloture de fenêtre/session
    .aggregate(0D)((k, newValue, aggValue) => (aggValue + newValue))

  kCA1.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire FF"))
  kCA1.toStream.foreach(
    (key, value) => println(s"clé de la fenêtre : ${key}"+
      s"clé du message : ${key.key()}"+
      s"debut : ${key.window().startTime().atOffset(ZoneOffset.UTC)}" +
      s"fin : ${key.window().endTime().atOffset(ZoneOffset.UTC)}")
  )



  // fenêtre glissante
  println("Fenêtre glissante")
  val kCA2 = kStrFacture
    .map((_,f) => ("1", f.total))
    .groupBy((k,t) => k)(Grouped.`with`(String, Double))
    .windowedBy(TimeWindows.of(Duration.ofSeconds(15)).advanceBy(Duration.ofSeconds(3)))
    //.windowedBy(TimeWindows.of(Duration.ofSeconds(15)).advanceBy(Duration.ofSeconds(3))
    // .grace(Duration.ofSeconds(3)))  // delais de cloture de fenêtre/session
    .aggregate(0D)((k, newValue, aggValue) => (aggValue + newValue))

  kCA2.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire  FG"))

  // fenêtre fixe
  println("Session")
  val kCA3 = kStrFacture
    .map((k,f) => (k, f.total))
    .groupBy((k,t) => k)(Grouped.`with`(String, Double))
    .windowedBy(SessionWindows.`with`(Duration.ofSeconds(15))) //par defaut les fenêtres/session ne sont pas cloturées
    //.windowedBy(SessionWindows.`with`(Duration.ofSeconds(15)).grace(Duration.ofSeconds(3)))  // delais de cloture de fenêtre/session
    .count()

  kCA3.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire par session"))

  // Calcul du chiffre d'affaire moyen
  val kCA4 = kStrFacture
    .map((_,f) => ("1", f))
    .groupBy((k,t) => k)
    .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
    .aggregate[Facture](Facture("", "", 0, 0D, OrderLine("", "", "", "", 0D, 0D, 0)))(
      (k, newFacture, aggFacture) =>  Facture(newFacture.factureId, "", newFacture.quantite + aggFacture.quantite, newFacture.total + aggFacture.total, OrderLine("", "", "", "", 0D, 0D, 0))
    ).mapValues(f => (f.quantite, f.total, f.total/f.quantite))
  kCA4.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire moyen tous les 5 secondes"))

  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }

}
