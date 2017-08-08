package org.sparktw.codefight

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest._
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.Seconds

class NGramsTest extends FunSuite with SharedSparkContext with RDDComparisons with TimeLimitedTests with Matchers {

  val timeLimit = Span(5, Seconds)

  test("Sample bi-grams test"){
    val inputStr: String = """Spark SQL is a Spark module for structured data processing.
                              |Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide 
                              |Spark with more information about the structure of both the data and the 
                              |computation being performed. Internally, Spark SQL uses this extra information
                              |to perform extra optimizations. There are several ways to interact with Spark SQL
                              |including SQL and the Dataset API. One use of Spark SQL is to execute SQL queries.
                              |Spark SQL can also be used to read data from an existing Hive installation.
                              |When running SQL from within another programming language the results will 
                              |be returned as a Dataset/DataFrame. You can also interact with the SQL interface
                              |using the command-line or over JDBC/ODBC.""".stripMargin
    val inputRDD: RDD[String] = sc.parallelize(Seq(inputStr))
    val expected: Array[(String, Int)] = Array(("spark sql", 6), ("interact with", 2), ("and the", 2), ("sql is", 2), ("can also", 2))
    val res: Array[(String, Int)] = Solution.answer(inputRDD, 2, 5)
    assert(Succeeded === (res should equal (expected)))
  }

  test("Hidden bi-grams test"){
    val inputRDD: RDD[String] = sc.parallelize(Seq("""I have a pen. I have an apple. Uh! Apple-pen!
                                                      |I have a pen. I have a pineapple. Uh! Pineapple-pen!
                                                      |Apple-pen. Pineapple-pen. Uh! Pen-pineapple-apple-pen""".stripMargin))
    val expected: Array[(String, Int)] = Array(("i have", 4), ("have a", 3), ("a pen", 2))
    val res: Array[(String, Int)] = Solution.answer(inputRDD, 2, 3)
    assert(Succeeded === (res should equal (expected)))
  }

  test("Hidden uni-grams test"){
    val inputRDD: RDD[String] = sc.parallelize(Seq("""I have a pen. I have an apple. Uh! Apple-pen!
                                                      |I have a pen. I have a pineapple. Uh! Pineapple-pen!
                                                      |Apple-pen. Pineapple-pen. Uh! Pen-pineapple-apple-pen""".stripMargin))
    val expected: Array[(String, Int)] = Array(("have", 4), ("i", 4), ("a", 3), ("uh", 3), ("pen", 2),
                                               ("pineapple-pen", 2), ("apple-pen", 2)) 
    val res: Array[(String, Int)] = Solution.answer(inputRDD, 1, 7)
    assert(Succeeded === (res should equal (expected)))
  }
}
