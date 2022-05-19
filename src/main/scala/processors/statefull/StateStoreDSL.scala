package processors.statefull

import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties

// Ici on défini le type du state store utilisé pas kafka streams
object StateStoreDSL extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val props_state: java.util.Map[String, String] = new java.util.HashMap[String, String]()
  props_state.put("retention.ms", "172800000") // nombre de jour de retention
  props_state.put("retention.bytes", "10000000000") // la taille qu'il ne faut pas dépasser
  props_state.put("cleanup.policy", "compact") //(ici seul le dernier élément est conserrvé) politique pour vider le topic changelog
  props_state.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000") // interval du commit dans le topic changelog (si c'est fréquent ça affecte la rapidité d'execution)
  props_state.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "10485760")

  val factureStoreName: String = "facturestore"
  val factureStoreSupplier: KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(factureStoreName)
  //val factureStoreBuilder : KeyValueStoreBuilder[ByteArrayValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreBuilder, String, string)
  val factureStoreBuilder: StoreBuilder[KeyValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreSupplier, String, String)

  // Store associer à la topologie
  val str: StreamsBuilder = new StreamsBuilder
  str.addStateStore(factureStoreBuilder) // Dans ce cas nous n'avons pas le contrôle du Store (lié à la topologie). C'est kafkaStreams qui se charge de manipuler le store


  // Création d'un State Store à l'initialisation d'un KTable
  val ktbl1: KTable[String, String] = str.table("ktabletest1", Materialized.as(factureStoreSupplier)(String, String)
    .withCachingEnabled() //haute disponibilité
    .withLoggingEnabled(props_state) // tolérence aux pannes
  )


  // Traitement
  ktbl1.toStream.print(Printed.toSysOut().withLabel("Clé/Valeur de KTable"))


  val topologie: Topology = str.build()
  val kkStream: KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }
}
