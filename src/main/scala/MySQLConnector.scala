import org.apache.spark.sql.{SaveMode, SparkSession}
import java.text.DateFormat

object MySQLConnector extends App {

  System.setProperty("hadoop.home.dir", "C:/Hadoop/")

  val ss = SparkSession.builder()
    .appName("Mon Job Spark")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    //.enableHiveSupport()
    .getOrCreate()

  val df_mysql= ss.read
    .format("jdbc")
    .option("url", "jdbc:mysql://127.0.0.1:3306")
    .option("dbtable","jea_db.orders")
    .option("user", "consultant")
    .option("password", "pwd#86")
    .option("dbtable","(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) requete")
    .load()
//df_mysql.show()
 /* df_mysql.write
    .mode(SaveMode.Overwrite)
    .format("com.databricks.spark.csv")
    .option("header", true)
    .save("C:\\Users\\PLB\\Downloads\\data\\commandes_totales.csv")

  df_mysql
    .coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .mode(SaveMode.Overwrite) */

  df_mysql
    //.repartition(1)
    .write
    .partitionBy("city")
    .format("com.databricks.spark.csv")
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .save("C:\\Users\\PLB\\Downloads\\data\\commandes_totales")
}
