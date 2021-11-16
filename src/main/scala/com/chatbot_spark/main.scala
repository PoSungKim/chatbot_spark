package com.chatbot_spark

import org.apache.spark.SparkContext

object main extends App {
  println("Hello World");

  val sparkContext = new SparkContext("local", "chatbot_spark");
  val sourceRDD = sparkContext.textFile("/Users/posungkim/Desktop/Portfolio/Big_Data_Platform/Files/words.txt")
  sourceRDD.take(1).foreach(println)

}
