package org.sparktw.codefight

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.Seconds

class InvertedIndexTest extends FunSuite with SharedSparkContext with RDDComparisons with TimeLimitedTests {

  val timeLimit = Span(5, Seconds)

  test("Sample Test"){
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles("data/file1.txt,data/file2.txt")
    val expectedRDD: RDD[(String, Set[String])] = sc.parallelize(Array(("Spark", Set("file1.txt", "file2.txt")),
        ("in-memory", Set("file2.txt")),
        ("is", Set("file1.txt")),
        ("advanced", Set("file2.txt")),
        ("general-purpose", Set("file1.txt")),
        ("that", Set("file2.txt")),
        ("a", Set("file1.txt")),
        ("fast", Set("file1.txt")),
        ("Apache", Set("file2.txt", "file1.txt")),
        ("data", Set("file2.txt")),
        ("flow", Set("file2.txt")),
        ("DAG", Set("file2.txt")),
        ("cluster", Set("file1.txt")),
        ("execution", Set("file2.txt")),
        ("computing", Set("file2.txt", "file1.txt")),
        ("acyclic", Set("file2.txt")),
        ("an", Set("file2.txt")),
        ("and", Set("file2.txt", "file1.txt")),
        ("supports", Set("file2.txt")),
        ("engine", Set("file2.txt")),
        ("system", Set("file1.txt")),
        ("has", Set("file2.txt"))))
    val resRDD: RDD[(String, Set[String])] = Solution.answer(inputRDD)
    assert(None === compareRDD(resRDD, expectedRDD))
  }

  test("Hidden Test Case"){
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles("data/ppap1.txt,data/ppap2.txt,data/ppap3.txt")
    val expectedRDD: RDD[(String, Set[String])] = sc.parallelize(Array(("pen", Set("ppap1.txt", "ppap2.txt")),
        ("a", Set("ppap1.txt", "ppap2.txt")),
        ("I", Set("ppap1.txt", "ppap2.txt")),
        ("Pen-Pineapple-Apple-Pen", Set("ppap3.txt")),
        ("an", Set("ppap1.txt")),
        ("have", Set("ppap2.txt", "ppap1.txt")),
        ("pineapple", Set("ppap2.txt")),
        ("Apple-pen", Set("ppap3.txt", "ppap1.txt")),
        ("apple", Set("ppap1.txt")),
        ("Pineapple-pen", Set("ppap3.txt", "ppap2.txt"))))
    val resRDD: RDD[(String, Set[String])] = Solution.answer(inputRDD)
    assert(None === compareRDD(resRDD, expectedRDD))
  }
   
}
