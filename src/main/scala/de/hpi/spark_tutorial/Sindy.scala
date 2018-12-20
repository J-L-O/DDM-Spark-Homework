package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    var dfs = List[DataFrame]()

    for (i <- inputs.indices){
      dfs = dfs ::: List(spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(inputs(i))
        .toDF())
    }

    val tuples = dfs.flatMap(
      t => t.columns
        .map(
          col => t
            .select(col)
            .distinct
            .withColumn("Colname", lit(col))
            .toDF("Value", "Colname")
        )
    ).reduce((a, b) => a.union(b))

    def inclusionLists(list: Set[String]): Array[(String, Set[String])] = {
      var result = Array[(String, Set[String])]()

      for (i <- list){
        result = result :+ (i, list - i)
      }

      result
    }

    val result = tuples
      .as[(String, String)]
      .groupByKey(t => t._1)
      .mapGroups{ (key, iterator) => (key, iterator
        .map(t => Set(t._2))
        .reduce((a,b) => { a union b })) }
      .map(t => inclusionLists(t._2))
      .toDF("InclusionLists")
      .withColumn("InclusionLists", explode($"InclusionLists"))
      .select("InclusionLists.*")
      .as[(String, Set[String])]
      .groupByKey(t => t._1)
      .mapGroups{ (key, iterator) => (key, iterator
        .map(t => t._2)
        .reduce((a,b) => { a intersect b })) }
      .toDF("Column", "Inclusion")
      .filter(size($"Inclusion") > 0)
      .as[(String, Set[String])]
      .map(t => (t._1, t._2.mkString(", ")))
      .toDF("Column", "Inclusion")
      .sort($"Column")
      .collect()

    result.foreach(t => println(t(0) + " < " + t(1)))
  }
}
