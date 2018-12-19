package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    var path = "./TPCH"
    var cores = 4
    args.sliding(2, 2).toList.collect {
      case Array("--path", argPath: String) => path = argPath
      case Array("--cores", argCores: String) => cores = argCores.toInt
    }

    println(path)
    println(cores)

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark

    val sparkBuilder = SparkSession
      .builder()
      .config("spark.driver.host", "localhost")
      .appName("SparkTutorial")
      .master(s"local[$cores]") // local, with <cores> worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", 2 * cores) //


    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    time {Sindy.discoverINDs(inputs, spark)}
  }
}
