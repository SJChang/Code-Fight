package org.sparktw.codefight

import org.apache.spark.rdd.RDD

object Solution {
  def answer(input: RDD[String], n: Int, topN: Int): Array[(String, Int)] = {
    val wordPattern = "[\\w-]+".r
    val result = input
      .flatMap(_.trim.split("[.!?]"))
      .flatMap(_.trim.split(" ")
        .map(wordPattern.findFirstIn(_).getOrElse().toString.toLowerCase)
        .sliding(n).filter(_.size == n))
      .map(_.mkString(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(topN)
    result 
  }
}
