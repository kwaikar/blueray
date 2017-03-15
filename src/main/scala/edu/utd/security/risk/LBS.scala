package edu.utd.security.risk

import scala.annotation.migration
import scala.annotation.varargs
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.runtime.universe
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import edu.utd.security.mondrian.DataWriter

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
object LBS {

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("Program variables expected : <HDFS_Data_file_path> <PREDICT_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <adversaryAttackCost> <USE_LSH(true/false)> <LSH_NUM_NEIGHBORS>")
    } else {
      val sc = SparkSession
        .builder.appName("LBS").master("local[4]").getOrCreate().sparkContext;
      var linesRDD = new DataReader().readDataFile(sc, args(0), true).cache();
      val i = 29779;
      lbs(linesRDD.lookup(i.longValue())(0), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble))
    }
  }

  /**
   * Using following singleton to retrieve/broadcast metadata variables.
   */
 
  def lbs(record: scala.collection.mutable.Map[Int, String], lbsParam: LBSParameters) {

    val algo = new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam);
    val optimalRecord = algo.findOptimalStrategy(record)
    println(optimalRecord._1 + " " + optimalRecord._2 + " =>" + optimalRecord._3);
  }

}