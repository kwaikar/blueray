package edu.utd.security.blueray

import java.lang.reflect.Field

import scala.collection.mutable.HashMap
import scala.util.control.Breaks._

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.InterruptibleIterator
import org.apache.spark.Partition
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.rdd.RDD
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

class AccessAuthorizer {
  def foo() = {
    println("Inside function")
  }
}

object AccessAuthorizer  {
  /**
   * Object of AccessAuthorizer class : used for testing the aspect functionality.
   */ 
    def driver()= {
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      /**
       * Test Collect, foreach and take functions
       */

      var policy = new Policy("hdfs://localhost/user/user_small.csv", "Lii");
      AccessAuthorizationManager.registerPolicy(policy);

      var inputFile = sc.textFile("hdfs://localhost/user/user_small.csv")
      println("Collecting stuff")
      inputFile.coalesce(1).saveAsTextFile("hdfs://localhost/user/user_authorized_single" + System.currentTimeMillis + ".csv");
      println("collect " + inputFile.collect().size)
      println("count " + inputFile.count())
      println("take(3) " + inputFile.take(3).size)
      println("takeSample " + inputFile.takeSample(false, 2, 0).size)
      println("countByValue " + inputFile.flatMap(_.split("\n")).countByValue())
      inputFile.foreach(println)
      println("reduceByKey " + inputFile.map(x => (x(1), 1)).reduceByKey(_ + _).collect().size)
      println("reduceLeft " + inputFile.map(x => (1)).collect().reduceLeft({ (x, y) => x + y }))
      inputFile.map(x => (x(1), 1)).collect().foreach(println)
      println("reduce " + inputFile.map(x => (1)).collect().reduce({ (x, y) => x + y }))
      println(inputFile.first().size)
      inputFile.saveAsTextFile("hdfs://localhost/user/user_authorized_single" + System.currentTimeMillis + ".csv");

      AccessAuthorizationManager.deRegisterPolicy(policy);
      println("count " + inputFile.count())

      val sqlcontext = new SQLContext(sc)

      case class Person(name: String, age: Int)

      /* // Create an RDD of Person objects and register it as a table.
    val people =( inputFile.map(_.split(",")).map(p => Person(p(0), p(3).trim.toInt))).toDF()
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 21 AND age <= 22")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)*/

    }
}
