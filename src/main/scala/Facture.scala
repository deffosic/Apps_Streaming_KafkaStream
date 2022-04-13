package schema

case class Facture(
                    factureId : String,
                    productName : String,
                    quantite : Int,
                    total : Double,
                    orderLine : OrderLine
                  )
case class OrderLine(
                      orderLineId : String,
                      productId : String,
                      billDate : String,
                      shipdate : String,
                      unitPrice : Double,
                      totalPrice : Double,
                      numUnits : Int
                    )