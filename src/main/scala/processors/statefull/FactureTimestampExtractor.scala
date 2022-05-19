package processors.statefull
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import schema.Facture

import java.text.SimpleDateFormat

class FactureTimestampExtractor extends TimestampExtractor {
  override def extract(consumerRecord: ConsumerRecord[AnyRef, AnyRef], l: Long): Long = {

    consumerRecord.value() match {
      case r: Facture => {
        //new SimpleDateFormat("dd/MM/yyy").parse(r.orderLine.billDate).toInstant.toEpochMilli
        val billDate = r.orderLine.billDate
        val formattedBillDate = new SimpleDateFormat("dd/MM/yyy")
        val transformBilldate = formattedBillDate.parse(billDate).toInstant.toEpochMilli
        transformBilldate
      }
      case _ => throw new RuntimeException(s"Erreur dans le parsing les messages ne sont pas des instances de Facture")

    }
  }
}
