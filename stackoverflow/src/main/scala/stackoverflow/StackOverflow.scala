package stackoverflow

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  //Local machine configurations
  @transient lazy val conf: SparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName("StackOverflow").
    set("spark.memory.fraction","0.8").
    set("spark.driver.memory","1500m").
    set("spark.memory.fraction","0.8").
    set("spark.executor.memory","1500m").
    set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

  @transient lazy val sc: SparkContext = SparkContext.getOrCreate(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)

    val grouped = groupedPostings(raw) //.sample(true,.005,0)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    //assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())


    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)

    sc.stop()
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] = {
      lines.map(line => {
        val arr = line.split(",")
        Posting(postingType = arr(0).toInt,
          id = arr(1).toInt,
          acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
          parentId = if (arr(3) == "") None else Some(arr(3).toInt),
          score = arr(4).toInt,
          tags = if (arr.length >= 6) Some(arr(5).intern()) else None)
      })

      //Hash partition
    }


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {

    //Create RDD pairs for partitioning
    val qid: RDD[(QID, Posting)] = postings.map(posting =>
      posting.postingType match {
        case 1 => (posting.id, posting)            // Question
        case 2 => (posting.parentId.get, posting)  // Answer
      }
    )

    //Create custom Range Partitioner
    val tunedPartitioner = new RangePartitioner(12, qid)
    val partitioned =  qid.partitionBy(tunedPartitioner)

    //Separate questions and answers
    val questions_qid: RDD[(QID, Question)] = partitioned.filter(_._2.postingType == 1)
    val answers_qid: RDD[(QID, Answer)] = partitioned.filter(_._2.postingType == 2)

    //Join with answers and aggregate
    def seqOp (acc: Iterable[(Question, Answer)], tpl: (Question, Answer)): Iterable[(Question, Answer)] = {
      acc ++ Iterable(tpl)
    }

    val combOp = (a: Iterable[(Question,Answer)],
                  b: Iterable[(Question,Answer)]) => a ++ b

    questions_qid.join(answers_qid).
      aggregateByKey(Iterable.empty[(Question,Answer)])( seqOp, combOp )
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
            if (score > highScore) highScore = score
            i += 1
          }
      highScore
    }

    def extractAnswers(iters: Iterable[(Question,Answer)]): Array[Answer] = {
      iters.map(tpl => tpl._2).toArray
    }

    grouped.map( (tpl: (QID, Iterable[(Question,Answer)])) => {

       val question_answers: Iterable[(Question,Answer)] = tpl._2
       val question: Question = question_answers.head._1
       val answers: Array[Answer] = extractAnswers(question_answers)

       (question, answerHighScore(answers))
     }
    )
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    // Make sure to cache these vectors because it will be accessed many times when calculating the centroids in the
    // kMeans function

    val vectors = scored.map( (tpl: (Question, HighScore)) =>
      ( firstLangInTag( tpl._1.tags, langs ).getOrElse(-1) * langSpread, tpl._2)
    )

    val tunedPartitioner = new RangePartitioner(12, vectors)
    vectors.partitionBy(tunedPartitioner).cache()

  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, "iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans = means.clone() // Need to compute newMeans

    // Group together all vectors to the closest mean to newMeans
    val closest: RDD[(MeanIndex, Score)]  = vectors.map( vector => (findClosest(vector,means), vector))
    // Calculate new averages for each cluster
    val clustersAverages: RDD[(MeanIndex, Score)] = closest.groupByKey().mapValues(averageVectors)

    // Update newMeans with new averages
    clustersAverages.collect().foreach( (tpl: (MeanIndex, Score)) =>
      newMeans(tpl._1) = tpl._2
    )

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int,Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {

    //Utility functions
    def lookupLang(langIndex: LangIndex): String = langs(langIndex/langSpread)

    def getTopLang(vs: Iterable[(LangIndex, HighScore)]): (Language, Percent) = {

      val top: (String, Int) = vs.map( (v: (LangIndex, HighScore)) => ( lookupLang(v._1),v._2) ). //Lookup the language associated with LangIndex
        groupBy(_._1).
        toList.
        map(v => (v._1, v._2.size)). //Performs SELECT count(*) GROUP BY Language
        maxBy(_._2)

      (top._1, top._2 / vs.size.toDouble * 100)

    }

    def median(numbers: Iterable[Int]): Int = {
      val mid = numbers.size/2
      val sorted = numbers.toSeq.sortWith(_ < _)

      if (sorted.size % 2 == 1) sorted(mid)
      else (sorted(mid - 1) + sorted(mid))/2
    }

    val closest: RDD[(MeanIndex, Score)] = vectors.map(score => ( findClosest(score,means), score))
    val closestGrouped: RDD[(MeanIndex, Iterable[Score])] = closest.groupByKey()

    val results = closestGrouped.mapValues { vs: Iterable[Score]  =>
      // get most common language in cluster and percent of the questions in the most common language
      val topLang: (Language, Percent) = getTopLang(vs)
      val clusterSize: Int    = vs.size
      val medianScore: Int    = median(vs.map(vector => vector._2))

      (topLang._1, topLang._2, clusterSize, medianScore)
    }

    results.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
