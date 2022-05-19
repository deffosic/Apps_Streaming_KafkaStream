package processors.statefull

import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import schema.Facture

class FactureTransformaSupplier extends ValueTransformerSupplier[String, String]{
  override def get(): ValueTransformer[String, String] = new ValueTransformer[String, String] {
    private var context : ProcessorContext = null
    private var factureStateStore: KeyValueStore[String, String] = null
    private var description = ""

    override def init(processorContext: ProcessorContext): Unit = {
      this.context = processorContext
      this.factureStateStore = this.context.getStateStore("facturestore").asInstanceOf[KeyValueStore[String, String]]
    }

    override def transform(v: String): String = {
      /**try{
        description = factureStateStore.get(v)
      } catch {

      }*/
      factureStateStore.putIfAbsent("45i", "Téléviseur plasma LG")
      factureStateStore.putIfAbsent("45a", "Smart one Refrigerator")
      factureStateStore.putIfAbsent("34b", "Living climatiseur")
      factureStateStore.putIfAbsent("48i", "Electrolux Machine à laver")

      if (factureStateStore.get(v) == null) {
        description = "Pas de valeurs correspondantes"
      }else {
        description = factureStateStore.get(v)
      }
      return description

    }

    override def close(): Unit = {
      if(this.factureStateStore.isOpen){
        this.factureStateStore.close()
      }
    }
  }
}
