import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object SparkProgram extends App {
  System.setProperty("hadoop.home.dir", "C:/Hadoop/")
  val ss = SparkSession.builder()
    .appName("Mon Job Spark")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    //.enableHiveSupport()
    .getOrCreate()

  //création d'un RDD

  val liste1 : List[Int] = List(1, 8, 5, 6, 9, 59, 23, 15, 4)
  val liste_names : List[String] = List("Aurelien", "Oxana", "Jose", "Joel", "Damien", "Juvenal", "Valentin", "Vincent", "Alexandar")

  val rdd_1 : RDD[String] = ss.sparkContext.parallelize(List("Aurelien", "Oxana", "Jose", "Joel", "Damien", "Juvenal", "Valentin", "Vincent", "Alexandar"))
  val rdd_2 : RDD[Int] = ss.sparkContext.parallelize(liste1)

  rdd_1.foreach(e =>println(e))


  val rdd_transform = rdd_1.map(e => e.length)
  rdd_transform.foreach(e => println(e))

  val rdd_file = ss.sparkContext.textFile("C:\\Users\\PLB\\Downloads\\data\\csv")
  rdd_file.foreach(e => println(e))

  val df_csv= ss.read
    .format("csv")
    .option("sep",",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("C:\\Users\\PLB\\Downloads\\data\\csv")
  //df_csv.show(30)

  val df_json = ss.read
    .format("json")
    .load("C:\\Users\\PLB\\Downloads\\data\\json")
  df_json.show(30)

  val df_parquet = ss.read
    .format("parquet")
    .load("C:\\Users\\PLB\\Downloads\\data\\parquet\\2010-summary.parquet")
  df_parquet.show(30)
  df_csv.printSchema()

  val df_csv2 = df_csv.select(
    col("DEST_COUNTRY_NAME"),
    col("count").cast(LongType)
  ).withColumn("ventes", col("count")*lit(10))
    .withColumn("concatenation", concat(col("count"), col("DEST_COUNTRY_NAME")))
    .withColumn("vente_category", when(col("count")< 1000, lit("MEDIOCRE")).otherwise(
      when(col("count").between(1000,2000), lit("A AMELIORER")).otherwise(
      when(col("count")>2000,lit("BRAVO")))))

  df_csv2.show(40)

  df_csv2.filter(col("DEST_COUNTRY_NAME") === lit("Egypt")&& col("vente_category").isin("A AMELIORER", "BRAVO") || col("ventes")<1000)
    .groupBy(col("DEST_COUNTRY_NAME"))
    .count()
    //.show()

  df_csv2
    .groupBy(col("DEST_COUNTRY_NAME"))
    .max("count").as("Maximum vendu par catégorie")
    //.show()

  val schema_orders= StructType(
    List(StructField("OrderId", IntegerType, false),
      StructField("customerId", IntegerType, true),
      StructField("campaignId", IntegerType, true),
      StructField("orderdate", TimestampType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("zipcode", StringType, true),
      StructField("paymenttype", StringType, true),
      StructField("totalprice", DoubleType, true),
      StructField("numorderlines", IntegerType, true),
      StructField("numunits", IntegerType, true)

    )
  )

  val df_orders= ss.read
    .format("csv")
    .schema(schema_orders)
    .option("sep","\t")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("C:\\Users\\PLB\\Downloads\\data\\orders.txt")

  df_orders.printSchema()
  df_orders.show(10)

  val schema_orderline = StructType(
    List(
      StructField("orderlineid", IntegerType, false),
      StructField("orderid", IntegerType, true),
      StructField("productid", IntegerType, true),
      StructField("shipdate", TimestampType, true),
      StructField("billdate", TimestampType, true),
      StructField("unitprice", DoubleType, true),
      StructField("numunits", IntegerType, true),
      StructField("totalprice", DoubleType, true)
    )
  )

  val df_orderline= ss.read
    .format("csv")
    .schema(schema_orderline)
    .option("sep","\t")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("C:\\Users\\PLB\\Downloads\\data\\orderline.txt")

  df_orderline.printSchema()
  df_orderline.show(10)

  val df_join= df_orders.join(df_orderline, df_orders("OrderId")=== df_orderline("orderlineid"), "inner")


  //df_join.printSchema()
  //df_join.show(10)

  df_orderline.createOrReplaceTempView("table_detailsCommande")
  df_orders.createOrReplaceTempView("table_commandes")

  val df_sql = ss.sql("SELECT * FROM table_detailsCommande dc INNER JOIN table_commandes tc ON dc.orderid = tc.orderid LIMIT 15")
  df_sql.show()
  df_sql.explain()

  df_sql
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .save("C:\\Users\\PLB\\Downloads\\data\\Damien\\parquet")


}
