package bench

import java.lang.NumberFormatException
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.io.Source
import scala.util.parsing.json.{JSON, JSONObject}


class InputDataException(message: String) extends Exception(message)

case class Codes(id: Int = -1, minutes_metric_1: Int = -1, minutes_metric_2: Int = -1)
case class DataRow(Date: String, input_name: String, input_id: Int, level_name: String, level_id: Int, low_name: String, low_id: Int,
                   activity_name: String, activity_id: Int, second_activity_id: Int, status: String, first_metric: Int, second_metric: Int)


object BenchTestApp {

  implicit def bool2int(bool: Boolean): Int = if (bool) 1 else 0
  val outputCols = Seq("Date", "input_name", "input_id", "level_name", "level_id", "activity_name", "activity_id", "v1", "v7",
                       "v30", "v90", "c1", "c7", "c30", "c90")
    
  def main(args: Array[String]): Unit = {

    val dataPath = "src/main/resources/data.csv"
    val vocabPath = "vocabulary.json"

    implicit val spark: SparkSession = getSpark

    val (valid, errored) = createOutput(vocabPath, dataPath)
    writeOutput(valid)

    spark.stop
  }

  def createOutput(vocabPath: String, dataPath: String)(implicit spark: SparkSession): Tuple2[DataFrame, DataFrame] = {
    val vocabJson = loadVocabularyJson(vocabPath)
    val vocab = buildVocabularyDataFrame(vocabJson)
    /* println(vocab.show()) */

    val data = loadData(dataPath)
    /* println(data.show()) */

    var joined = data.join(vocab, data.col("activity_id") === vocab.col("id"), "left_outer").na.fill(-1)
    val minsPerDay = 1440
    val numDays = Seq(1, 7, 30, 90)
    for (i <- numDays){
      joined = joined
        .withColumn(s"v$i", (
          (joined("minutes_metric_1") === minsPerDay * i).cast(IntegerType) * joined("first_metric")
        ).cast(IntegerType))
        .withColumn(s"c$i", (
          (joined("minutes_metric_2") === minsPerDay * i).cast(IntegerType) * joined("second_metric")
        ).cast(IntegerType))
    }
    /* println(joined.show()) */
    
    val condition = joined("activity_id").isNull.or(joined("input_id").isNull.or(joined("level_id").isNull.or(joined("id") === -1)))
    val errored = joined.where(condition === true)
    println(errored.show())
    val valid = joined.where(condition === false)
    println(valid.show())
    (valid, errored)
  }

  // def writeErrors(df: DataFrame): Unit = {
  //   df.select(.map(df(_)): _*).repartition(1).write
  //     .format("com.databricks.spark.csv")
  //     .option("header", "true")
  //     .save("src/main/resources/errors.csv")
  // }

  def writeOutput(df: DataFrame): Unit = {
    df.select(outputCols.map(df(_)): _*).repartition(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/main/resources/output.csv")
  }

  def loadData(dataPath: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv").option("header", "true").option("delimiter", ";").load(dataPath)
  }

  def getSpark: SparkSession = {
    SparkSession.builder.appName("Embedding").config("spark.master", "local").getOrCreate()
  }

  def buildVocabularyDataFrame(json: JSONObject)(implicit spark: SparkSession): DataFrame = { 
    import spark.implicits._
    val csvRows: Array[String] = json.obj("data").toString().split("\n")
    val csvData: Dataset[String] = spark.createDataset(csvRows)
    val csvCodes = spark.read.option("header", "true").option("inferSchema", "true").csv(csvData).toDF().select("Codes").rdd.map {
      row => row(0) match {
        case string: String => {
          val codeElements: Array[Int] = string.filterNot("() " contains _).split(";") map {
            field => try { field.toInt } catch {
              case e: NumberFormatException => -1
            }
          }
          Codes(codeElements(0), codeElements(1), codeElements(2))
        }
        case _ => Codes()
      }
    }
    spark.createDataFrame(csvCodes)
  }

  def loadVocabularyJson(jsonPath: String): JSONObject = {
    val resource = Source.fromResource(jsonPath)
    JSON.parseFull(resource.getLines.mkString("")).get match {
      case json: Map[String, Any] => new JSONObject(json)
      case _ => throw new Exception(s"File missing: $jsonPath")
    }
  }
}
