name := "spark_verif_connaissances"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// To create jar files.
// Go to your project as the same level of build.sbt
// Launch in cmd : $ sbt package


// The jar is now created in the targert folder
