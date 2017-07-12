package org.sparktw.codefight

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.Seconds

class WordCountTest extends FunSuite with SharedSparkContext with RDDComparisons with TimeLimitedTests {

  val timeLimit = Span(5, Seconds)

  test("Test Order By Key"){
    val inputRDD: RDD[String] = sc.parallelize(Seq("hello world"))
    val expectedRDD: RDD[(String, Int)] = sc.parallelize(List(("hello", 1), ("world", 1)))
    val resRDD: RDD[(String, Int)] = Solution.answer(inputRDD)
    assert(None === compareRDDWithOrder(resRDD, expectedRDD))
  }

  test("Test Order By Value"){
    val inputRDD: RDD[String] = sc.parallelize(Seq("hello world world"))
    val expectedRDD: RDD[(String, Int)] = sc.parallelize(List(("world", 2), ("hello", 1)))
    val resRDD: RDD[(String, Int)] = Solution.answer(inputRDD)
    assert(None === compareRDDWithOrder(resRDD, expectedRDD))
  }

  test("Test Same Value"){
    val inputRDD: RDD[String] = sc.parallelize(Seq("hello hello world world"))
    val expectedRDD: RDD[(String, Int)] = sc.parallelize(List(("hello", 2), ("world", 2)))
    val resRDD: RDD[(String, Int)] = Solution.answer(inputRDD)
    assert(None === compareRDD(resRDD, expectedRDD))
  }
   
}
