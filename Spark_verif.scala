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
