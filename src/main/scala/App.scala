package com.westermann.bench

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source
import scala.util.parsing.json.{JSON, JSONObject}


object BenchTestApp {

  implicit def bool2int(bool: Boolean): Int = if (bool) 1 else 0
  val outputCols = Seq("Date", "input_name", "input_id", "level_name", "level_id", "activity_name", "activity_id", "v1", "v7",
                       "v30", "v90", "c1", "c7", "c30", "c90")
    
  def main(args: Array[String]): Unit = {

    val dataPath = "src/main/resources/data.csv"
    val vocabPath = "vocabulary.json"

    implicit val spark: SparkSession = getSpark

    val vocabJson = loadVocabJson(vocabPath)
    val vocab = buildVocabData(vocabJson)
    val data = loadData(dataPath)

    val (valid, errored) = createOutput(vocab, data)

    writeOutput(valid, outputCols, "output")
    writeOutput(errored, data.columns, "errors")

    spark.stop()
  }

  def createOutput(vocab: DataFrame, data: DataFrame)(implicit spark: SparkSession): Tuple2[DataFrame, DataFrame] = {

    var joined = data.join(vocab, data.col("activity_id") === vocab.col("id"), "left_outer")
    val minsPerDay = 1440

    for (i <- Seq(1, 7, 30, 90)){
      joined = joined
        .withColumn(s"v$i", (
          (joined("minutes_metric_1") === minsPerDay * i).cast(IntegerType) * joined("first_metric")
        ).cast(IntegerType))
        .withColumn(s"c$i", (
          (joined("minutes_metric_2") === minsPerDay * i).cast(IntegerType) * joined("second_metric")
        ).cast(IntegerType))
    }
    
    val condition = joined("activity_id").isNull
      .or(joined("input_id").isNull
      .or(joined("level_id").isNull
      .or(joined("id").isNull)))
    val errored = joined.where(condition === true)
    val valid = joined.where(condition === false)

    (valid, errored)
  }

  def writeOutput(df: DataFrame, outputCols: Seq[String], name: String): Unit = {
    df.select(outputCols.map(df(_)): _*).repartition(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter",";")
      .mode("overwrite")
      .save(s"src/main/resources/$name.csv")
  }

  def loadData(dataPath: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv").option("header", "true").option("delimiter", ";").load(dataPath)
  }

  def getSpark: SparkSession = {
    SparkSession.builder.appName("BenchTest").config("spark.master", "local").getOrCreate()
  }

  def buildVocabData(json: JSONObject)(implicit spark: SparkSession): DataFrame = { 
    import spark.implicits._
    spark.sparkContext.parallelize(json.obj("data").toString().split("\n"))
      .map(_.split(",")(6).filterNot("() " contains _).split(";").padTo(3, ""))
      .map(r => (r(0), r(1), r(2))).toDF("id", "minutes_metric_1", "minutes_metric_2")
  }

  def loadVocabJson(jsonPath: String): JSONObject = {
    val resource = Source.fromResource(jsonPath)
    JSON.parseFull(resource.getLines.mkString("")).get match {
      case json: Map[String, Any] => new JSONObject(json)
      case _ => throw new Exception(s"Vocab invalid: $jsonPath")
    }
  }
}
