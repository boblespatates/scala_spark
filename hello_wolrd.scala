// import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
  val myRange = spark.range(1000).toDF("number")
  myRange.show()

  val divisBy2 = myRange.where("number % 2 = 0")
  divisBy2.show()


  /*
  // Create a SparkContext to initialize Spark
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)
  */

  /*
  // Load the text into a Spark RDD, which is a distributed representation of each line of text
  val textFile = sc.textFile("C:\\Users\\nzablocki\\IdeaProjects\\test_spark\\text_spark.txt")

  // Line count
  println(textFile.count())

  // First word
  println(textFile.first())

  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word,1))
    .reduceByKey(_ + _)

  counts.foreach(println)
  println(counts.count())
  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

   */

}
