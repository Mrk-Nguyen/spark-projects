package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow.rawPostings

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val linesSmall: RDD[String] = sc.textFile("src/test/resources/testdata.csv")
  val linesLarge: RDD[String] = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  override def afterAll() {
    sc.stop()
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("grouped postings return proper grouping counts") {
    val raw = testObject.rawPostings(linesSmall)
    val grouped = testObject.groupedPostings(raw)
    val groupedCount = grouped.map(group => (group._1, group._2.size)).collect().toMap

    assert(groupedCount.getOrElse(9, None) === 3, "Question 9 should contain 3 answers")
    assert(groupedCount.getOrElse(99, None) === None, "Question 99 should not be present")
    assert(groupedCount.getOrElse(999, None) === 5, "Question 999 should contain 5 answers")
    assert(groupedCount.getOrElse(9999, None) === 3, "Question 9999 should contain 3 answers")
  }


  test("Scored postings return proper scores") {

    val raw = testObject.rawPostings(linesSmall)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped).collect()

    val evalScores = Map(9 -> 21, 999 -> 89, 9999 -> 34, 3838 -> 2)
    scored.foreach(tpl => assert(tpl._2 === evalScores(tpl._1.id)))
  }

  test("Vector postings return proper LangIndex and HighScores for small file list") {
    val raw = testObject.rawPostings(linesSmall)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped)
    val vectors = testObject.vectorPostings(scored).collect()

    assert(vectors(0).toString() === "(-50000,2)")
    assert(vectors(1).toString() === "(0,89)")
    assert(vectors(2).toString() === "(500000,21)")
    assert(vectors(3).toString() === "(500000,34)")
  }

  ignore("Vector postings return proper vector size for huge file list") {
    val raw = testObject.rawPostings(linesLarge)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped)
    val vectors = testObject.vectorPostings(scored)

    assert(vectors.count() === 2121822, "Incorrect number of vectors")
  }

  ignore("Kmeans function works") {
    val raw = testObject.rawPostings(linesLarge)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped).sample(true, 0.025, 0)
    val vectors = testObject.vectorPostings(scored)
    val means = testObject.kmeans(testObject.sampleVectors(vectors),vectors,debug = true)
    assert(means.size === 45, "means size should be 45")
  }
}
