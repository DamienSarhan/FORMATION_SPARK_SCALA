package org.ALTEN
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object MyFirstProject extends App {

  System.setProperty("hadoop.home.dir", "C:/Hadoop/")
  println( "Hello World!" )

  val spark = SparkSession.builder.appName("ma_session_spark").getOrCreate( )
  val df = spark.read.csv("README.txt")
  df.show()


}
