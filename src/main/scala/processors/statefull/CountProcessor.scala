package processors.statefull

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}

import java.util.Properties

object CountProcessor extends App {
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._

    val props : Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,"count-processor")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

    val str : StreamsBuilder = new StreamsBuilder
    val kStr : KStream[String, String] = str.stream[String, String]("streams_app")

    val kStrMaj :  KStream[String, String] = kStr.mapValues(v => v.toUpperCase)

    val kCount :  KTable[String, Long] = kStrMaj.flatMapValues( r => r.split(","))
      .groupBy((_, valeur) => valeur)
      .count()(Materialized.as("counts-strore"))

    kCount.toStream.to("stream_count")



    val topologie : Topology = str.build()
    val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
    kkStream.start()

    sys.ShutdownHookThread{
      kkStream.close()
    }


}
