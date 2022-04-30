import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}
import schema.Facture

import java.util.Properties

object StateStoreDSL extends  App{
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"ktable-test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  /*val props_state : java.util.Map[String, String] = new java.util.HashMap[String, String]()
  props_state.put("retention.ms","172800000")
  props_state.put("retention.bytes","10000000000")
  props_state.put("cleanup.policy", "compact")
  props_state.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
  props_state.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "10485760")*/

  val factureStoreName: String  = "facturestore"
  val factureStoreSupplier: KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(factureStoreName)
  //val factureStoreBuilder : KeyValueStoreBuilder[ByteArrayValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreBuilder, String, string)
  val factureStoreBuilder : StoreBuilder[KeyValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreSupplier, String, String)

  // Store associer à la topologie
  val str : StreamsBuilder = new StreamsBuilder
  str.addStateStore(factureStoreBuilder) // Dans ce cas nous n'avons pas le contrôle du Store (lié à la topologie). C'est kafkaStreams qui se charge de manipuler le store


  // Création d'un State Store à l'initialisation d'un KTable
  val ktbl1 : KTable[String, String] = str.table("ktabletest1", Materialized.as(factureStoreSupplier)(String, String)
  //.withCachingEnabled() //haute disponibilité
  //.withLoggingEnabled(props_state) // tolérence aux pannes
  )


  // Traitement
  ktbl1.toStream.print(Printed.toSysOut().withLabel("Clé/Valeur de KTable"))



  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    kkStream.close()
  }
}
