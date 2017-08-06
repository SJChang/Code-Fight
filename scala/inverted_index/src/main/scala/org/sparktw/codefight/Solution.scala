package org.sparktw.codefight

import org.apache.spark.rdd.RDD

object Solution {
  def answer(input: RDD[(String, String)]): RDD[(String, Set[String])] = {
    val filePattern = "\\w+\\.txt".r
    val wordPattern = "[\\w-]+".r
    val result = input.map { case(file, text) =>
      (filePattern.findFirstIn(file).get, text.trim.split("\n").flatMap(line => line.split(" ")))
      }
      .flatMap{ case(file, words) =>
         words.map(word => ((wordPattern.findFirstIn(word).get, Set(file))))
      }
      .reduceByKey((a, b) => (a ++ b))
      
    result 
  }
}
