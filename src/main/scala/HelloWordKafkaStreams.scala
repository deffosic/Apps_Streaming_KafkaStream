import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.scala.kstream.KGroupedStream
import org.apache.kafka.streams.scala.kstream.KGroupedTable
import org.apache.kafka.streams.scala.kstream.SessionWindowedKStream
import org.apache.kafka.streams.scala.kstream.TimeWindowedKStream
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state._

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}


object HelloWordKafkaStreams {

  def main(args : Array[String]) : Unit = {

    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes.String

    val str : StreamsBuilder = new StreamsBuilder()
    val kstr : KStream[String, String] = str.stream("stream_topic")
    val kstrMaj : KStream[String, String] = kstr.mapValues(v => v.toString)//kstr.map((_, v) => (_, v.toUpperCase))
    kstrMaj.to("stream_topic_upper")

    val topologie : Topology = str.build()
    val kkstream : KafkaStreams = new KafkaStreams(topologie, getParams("App_stream", "localhost:9092"))
    kkstream.start()

    sys.ShutdownHookThread{
      kkstream.close()
    }

  }

  def getParams(idApp : String, bootstrapServer : String) : Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, idApp)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.streams.scala.Serdes.String")
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.streams.scala.Serdes.String")
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once")

    props
  }

}
