package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import scala.util.Random

object FileCreator {

  def main(args: Array[String]): Unit = {

    var sc: SparkContext = SparkSession
      .builder.appName("FileCreator").master(args(0)).getOrCreate().sparkContext;
    sc.setLogLevel("ERROR");
    val numbers = sc.parallelize(Array(1 to 50));
    val numRecordsPerPartition = args(1).toInt;
    val races  =Array("Asian-Pac-Islander","Black","White","Amer-Indian-Eskimo","Other");
    val genders  =Array("Male","Female");
    val rdd = numbers.flatMap(x => {
      val list = ListBuffer[String]();

      for (i <- 0 to numRecordsPerPartition) {

        val age = 0 + (120.0) * Math.random();
        val zip = 37010.0 + (35328.0) * Math.random();
        val race = races( Math.round(5.0 * Math.random()).intValue());
        val gender = genders(Math.round(2.0 * Math.random()).intValue());

        list.+(gender + "," + zip + "," + age + race);
      }
      list

    });
    rdd.saveAsTextFile(args(2));
println("file written to "+args(2));
  }
}
