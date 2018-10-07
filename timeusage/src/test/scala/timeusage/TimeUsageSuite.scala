package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new TimeUsage {

    override lazy val spark =
      SparkSession
        .builder()
        .appName("TimeUsageTest")
        .config("spark.master", "local[6]")
        .getOrCreate()
  }

  import testObject.spark.implicits._

  val surveySource = "/timeusage/atussum.csv"
  val surveySourceSample = "/atussum-sample.csv"


  override def afterAll(): Unit = {
    testObject.spark.stop()
  }

  ignore("testObject can be instantiated") {
    val instatiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instatiatable, "Can't instantiate a TimeUsage object")
  }

  ignore("dfSchema should return a StructType given List[String]") {

    val columns = List("col1","col2","col3","col4","col5")
    val struct = testObject.dfSchema(columns)

    assert(struct.fieldNames.sameElements(columns), "fieldnames should match the given column list")
    struct.tail.foreach(field => assert(field.dataType.isInstanceOf[DoubleType], "field dataType should be DoubleType"))
  }

  ignore("row function returns a Row") {

    val file = Source.fromFile(testObject.fsPath(surveySourceSample)).getLines().toList

    val record = file(1).split(",").toList
    val aRow = testObject.row(record)

    assert(aRow.size === 455,"Number of columns should be 455")
    assert(aRow.getString(0) === "\"20030100013280\"", "First element should equal the \"20030100013280\"")
    assert(aRow.getDouble(454) === 0.0, "Last element should equal to the double 0")
  }

  ignore("Read returns correct columns and correct count") {
    val (columns, dataFrame) = testObject.read(surveySourceSample)

    assert(columns.size === 455, "Column count should be 455")
    assert(dataFrame.count() === 100, "DataFrame count should be 100")
  }

  ignore("classifiedColumns return the right number of groupings") {
    val (columns, dataFrame) = testObject.read(surveySourceSample)

    val (primary,working,other) = testObject.classifiedColumns(columns)

    assert(primary.size === 55, "Primary grouping should contain 55 columns")
    assert(working.size === 23, "Working grouping should contain 2 columns")
    assert(other.size === 354, "Other grouping should contain 354 columns")
  }

}
