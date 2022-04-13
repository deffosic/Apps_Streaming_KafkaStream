import com.fasterxml.jackson.annotation.JsonInclude

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import schema.{Facture, OrderLine}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

object TestSerDes extends App{

  val facture1 = Facture("a320", "Samsung TV", 2, 700.0, OrderLine("tv123","456","12/2/2012","12/2/2012",350.0,700.0,2))

  val objetMapper : ObjectMapper = new ObjectMapper()
  objetMapper.registerModule(DefaultScalaModule) // charge les types dans le registre de scala necessaire uniquement à la désérialisation

  //paramètre sérialisation
  objetMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)  // accept les attributs null
  objetMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
  objetMapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)

  //paramètre sérialisation
  objetMapper.configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, true)
  objetMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) // permet d'accepter des attributs inconnus du schema

  val s = objetMapper.writeValueAsString(facture1)
  println("sérialisation de facture : " + s)

  val sb = objetMapper.writeValueAsBytes(facture1)
  println("sérialisation de facture : " + sb)

  val d = objetMapper.readValue(s, classOf[Facture])
  val db= objetMapper.readValue(sb, classOf[Facture])

  println(db)

  val bts = new ByteArrayOutputStream()
  val ost = new ObjectOutputStream(bts)


  ost.writeObject(facture1)
  println(bts.toByteArray)

  val btos = new ByteArrayInputStream(bts.toByteArray)
  val oist = new ObjectInputStream(btos)
  val facture2 = oist.readObject().asInstanceOf[Facture]

  println(facture2)

}
