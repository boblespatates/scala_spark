import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object Spark_verif {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Geo loc application")
      .config("spark.master", "local")
      .getOrCreate()

    val filepath = "C:\\Users\\nzablocki\\Documents\\geolocation.csv"

    val dfGeo = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filepath)

    dfGeo.show()

    println("columns func")
    println(dfGeo.columns)

    println("printSchema func")
    dfGeo.printSchema()

    println("Describe func")
    dfGeo.describe().show()

  }
}


/* Uber use case


 // SparkSession definition
  val spark = SparkSession
    .builder
    .appName("Simple Application")
    .config("spark.master", "local")
    .getOrCreate()

  // Define the Schema of inout files
  val schema = StructType(Array(
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base",  StringType, true)
  ))

  // Path of the input file
  //val input_filepath = "C:\\Users\\nzablocki\\Documents\\uber_use_case\\input\\uber-raw-data-apr14.csv"
  val input_filepath = "hdfs:///tmp/data/uber-raw-data-apr14.csv"

  // Load Dataset
  val df = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(input_filepath)

  println("df")
  df.show()

  // Time is not well infered, so there is a need to change the schema
  val df_clean = df.select(df("Lat"),
    df("Lon"),
    df("Base"),
    unix_timestamp(df("Date/Time"), "MM/dd/yyyy HH:mm:ss").cast(TimestampType).as("timestamp")
  )

  // Display DataFrame info
  df_clean.cache()
  df_clean.show()
  df_clean.printSchema()

  // Put all the features of a row in a vector
  val featureCols = Array("Lat", "Lon")
  val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
  val df2 = assembler.transform(df_clean)

  df2.show()

  // Clustering Part
  val Array(trainingData, testData) = df2.randomSplit(Array(0.7,0.3),5043)
  val kmeans = new KMeans().setK(20).setFeaturesCol("features").setMaxIter(5)
  val model = kmeans.fit(trainingData)

  // Display clusters coordinates
  println("Final Centers: ")
  model.clusterCenters.foreach(println)

  // Evaluate the model
  val categories = model.transform(testData)
  categories.show()

  // Which hour of the day and which cluster has the hiest amount of data
  categories.select(hour(categories("timestamp")).alias("hour"), categories("prediction"))
    .groupBy("hour", "prediction")
    .agg(count("prediction").alias("count"))
    .orderBy(desc("count"))
    .show()

  categories.groupBy("prediction").count().show()

*/
