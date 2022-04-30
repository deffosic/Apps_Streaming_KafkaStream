import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KTable, Materialized, Produced}
import schema.Facture
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}
import serdes.{JSONDeserializer, JSONSerializer}

import java.util
import java.util.Properties

object StataStoreProcessor extends  App{
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  implicit val jsonSerdes : Serde[Facture] = Serdes.serdeFrom[Facture](new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(String, jsonSerdes)
  implicit val producer : Produced[String, Facture] = Produced.`with`(String, jsonSerdes)

  val props : Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,"ktable-test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")

  val props_state : java.util.Map[String, String] = new util.HashMap[String, String]()
  props_state.put("retention.ms","172800000")
  props_state.put("retention.bytes","10000000000")
  props_state.put("cleanup.policy", "compact")
  props_state.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
  props_state.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "10485760")

  val factureStoreName: String  = "facturestore"
  val factureStoreSupplier : KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(factureStoreName)
  //val factureStoreBuilder : KeyValueStoreBuilder[ByteArrayValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreBuilder, String, string)
  val factureStoreBuilder : StoreBuilder[KeyValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreSupplier, String, String).withCachingEnabled().withLoggingEnabled(props_state)
    //.withCachingEnable() // Haute disponibilité
   // .withLoggingEnable(props_state) // tolérence aux pannes


  // Store libre de toute topologie et peut être associé à un processor
  val factureStore : KeyValueStore[String, String] = factureStoreBuilder.build() //pensez à fermer à la fin avant la fin de l'instance

  factureStore.put("FR", "France")
  val etat = factureStore.get("FR")

  val str : StreamsBuilder = new StreamsBuilder

  // Création d'un State Store à l'initialisation d'un KTable
  val ktbl1 : KTable[String, String] = str.table("ktabletest1", Materialized.as(factureStoreBuilder))


  // Traitement
  ktbl1.toStream.print(Printed.toSysOut().withLabel("Clé/Valeur de KTable"))



  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread{
    factureStore.close()
    kkStream.close()
  }
}
