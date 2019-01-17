import com.westermann.bench.BenchTestApp
import org.scalatest.FlatSpec


class BenchTestAppSpec extends FlatSpec {
  val app = BenchTestApp
  import app._
  implicit val spark = getSpark

  "createOutput" should "create the correct two output frames" in {
    val dataPath = "src/test/resources/test_data.csv"
    val vocabPath = "test_vocabulary.json"
    val vocabJson = loadVocabJson(vocabPath)
    val vocab = buildVocabData(vocabJson)
    val data = loadData(dataPath)
    val (valid, errored) = createOutput(vocab, data)

    val expectedValid = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("src/test/resources/test_output.csv")
    val expectedErrors = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("src/test/resources/test_errors.csv")

    println(valid.show())
    println(errored.show())
    assert(valid.count() === expectedValid.count())
    assert(errored.count() === expectedErrors.count())
  }
}
