case class Vehicule(
                   Matricule_ID : String,
                   Couleur : String,
                   Transmission_type : String,
                   Moteur_type : String

                   )
{
  def accelerer() : Unit = {

  }
  def freiner() : Unit = {

  }
  def VitesseMax(type_moteur: String, transmission: String): Int={
    150
  }
}
