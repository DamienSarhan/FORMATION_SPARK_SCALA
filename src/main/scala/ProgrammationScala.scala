//import scala.collection.mutable._
import org.apache.spark.sql.catalyst.expressions.codegen.TrueLiteral

import scala.collection.immutable._

object ProgrammationScala {
  val session_kafka : Int = 15
  val honda_civic: Vehicule = new Vehicule(Matricule_ID = "195354", Couleur = "Red", Transmission_type = "Auto", Moteur_type =" 4 cylindres 2.0L turbo")

def main ( args: Array[String]) : Unit = {
  println("Hello World, SCALA")
  honda_civic.Couleur : String ; honda_civic.Couleur
  honda_civic.accelerer()
  honda_civic.VitesseMax(type_moteur = "", transmission = ""  )
  fonction2()
  StructureConditionnelles()
  println("la couleur de la renault mégane est "+ honda_civic.Couleur)
  StructureDataScala()
}
  def fonction2() : Unit = {
    println("valeur de la variable :"+ session_kafka)
  }

  def StructureConditionnelles ( ) : Unit = {
    var i = 0
    while (i<7) {
      println(i)
      i=i+1
    }
  }
def StructureDataScala() : Unit ={
  //liste
  val liste1 : List[Int] = List(1, 8, 5, 6, 9, 59, 23, 15, 4)
  val liste_names : List[String] = List("Aurelien", "Oxana", "Jose", "Joel", "Damien", "Juvenal", "Valentin", "Vincent", "Alexandar")
  val nums = List.range(0,15)

  for (name <- liste_names) {
    println(name)
    println("=================")
  }
  val liste_maj : List[String] = liste_names.map(e => e.toUpperCase)
  liste_maj.foreach(e => println(e))
  println("=================")

  val list_v = liste_names.map(l => l.startsWith("J"))
  list_v.foreach(l=> println(l))
  println("===============")

  val list_double = liste1.map(2*_/*x => 2*x*/)
  list_double.foreach(k => println(k))
  println("==============")

  val list_sup5 = liste1.filter(_> 5)
  list_sup5.foreach(e => println(e))
  println("==============")

  //tuple
  val non_tuple : (String, Int, Boolean) = ("jvc", 40, true)
  val tuple2 = (45, " lion", "terre", false)
  //val vehicule2 = ("Moteur diesel", " Automatic", Vehicule("143242", "red", "auto","4cyl 2.0L turbo", true))
  println(tuple2._3.toUpperCase)
  //vehicule2._3.Moteur_type

  //Map
  val villes : Map[String, String] = Map(
    "PS" -> "Paris",
    "LS" -> "Lyon",
    "MA" -> "Marseille"
  )

  villes.foreach(k =>println("clé" + k._1 + "valeur" + k._2))

  //Tableau
  val tab : Array[String] = Array("Aurelien", "Oxana", "Jose", "Joel", "Damien", "Juvenal", "Valentin", "Vincent", "Alexandar")
  tab(0) = "Julien"
  for (i <- 0 to 6) {
    println(tab(i))
  }

}



}
