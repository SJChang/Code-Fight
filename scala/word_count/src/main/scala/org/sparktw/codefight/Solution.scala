package org.sparktw.codefight

import org.apache.spark.rdd.RDD

object Solution {
  def answer(data: RDD[String]): RDD[(String, Int)] = {
    val result = data.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._1)
      .sortBy(x => x._2, false)
    result
  }
}
