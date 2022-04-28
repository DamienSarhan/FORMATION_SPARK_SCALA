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




}
