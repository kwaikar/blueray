package edu.utd.security.lbs

import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.util.Collections

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
class LBS {

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR");
    lbs(args(0), args(1), args(2), args(3).toInt);
  }

  var metadataFilePath: String = null;
  var dataReader: DataReader = null;
  var linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
  var sc: SparkContext = SparkSession
    .builder.appName("LBS").master("local[2]").getOrCreate().sparkContext;

  def getFirstDataLine(): (Long, scala.collection.mutable.Map[Int, String]) = {
    return linesRDD.first();
  }
  /**
   * Using following singleton to retrieve/broadcast metadata variables.
   */
  object Metadata {
    @volatile private var metadata: Broadcast[Metadata] = null;
    def getInstance(sc: SparkContext, dataReader: DataReader, filePath: String): Broadcast[Metadata] = {
      if (metadata == null) {
        synchronized {
          if (metadata == null) {

            val metadataVal = dataReader.readMetadata(filePath);
            metadata = sc.broadcast(metadataVal)
          }
        }
      }
      metadata
    }
  }

  def lbs(hdfsDataFilePath: String, metadataFilePath: String, outputFilePath: String, k: Int) {

    val dataReader = new DataReader(sc);

    linesRDD = dataReader.readDataFile(hdfsDataFilePath, true);
    linesRDD.cache();
    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    val record = linesRDD.first();
    println("Is first record leaf : " + isGLeafNode(record._2));
    var newMap = new HashMap[Int, String]();
    for (i <- 0 to metadata.value.numColumns() - 1) {

      if (metadata.value.getMetadata(i).get.getColType() == 's') {
        newMap.put(i, metadata.value.getMetadata(i).get.getRootCategory().value());
      } else {
        newMap.put(i, metadata.value.getMetadata(i).get.getMin() + "_" + metadata.value.getMetadata(i).get.getMax());
      }
      println("Putting+ " + i + " - " + newMap.get(i).get)
    }

    println("Is Leaf record leaf : " + isGLeafNode(newMap));

    val optimalRecord = findOptimalStrategy(record)
  }

  def isGLeafNode(map: scala.collection.mutable.Map[Int, String]): Boolean =
    {
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);

      for (i <- 0 to metadata.value.numColumns() - 1) {
        if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
          val column = metadata.value.getMetadata(i).get;
          val value = map.get(i).get.trim()
          print(column.getIndex() + " " + value);
          if (!value.equalsIgnoreCase("*")) {
            if (column.getColType() == 's') {
              if (!value.equalsIgnoreCase(column.getRootCategory().value())) {
                return false;
              }
            } else {
              if (value.contains("_")) {
                val range = value.split("_");
                if (!(range(0).toDouble == column.getMin() && (range(1).toDouble == column.getMax()))) {
                  return false;
                }
              } else {
                return false;
              }
            }
          }
        }
      }
      return true;
    }
  def getPublishersLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      return 1;
    }
  val MAX_PUBLISHER_PROFIT = 0;

  def getPublishersBenefit(g: scala.collection.mutable.Map[Int, String]): Double =
    {

      return MAX_PUBLISHER_PROFIT * (1 - (getInformationLoss(g) / getMaximulInformationLoss(g)));
    }

  def getInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {

      return 1;
    }
  def getMaximulInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {

      return 1;
    }

  def getPiOfG(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      return 1;
    }
  def getChildren(g: scala.collection.mutable.Map[Int, String]): List[scala.collection.mutable.Map[Int, String]] =
    {
      /**
       * Iterate over each attribute, generalize the value one step up at a time, accumulate and return the list.
       */
      val list = List[scala.collection.mutable.Map[Int, String]]();
      return list.+:(g);
    }
  def findOptimalStrategy(top: (Long, scala.collection.mutable.Map[Int, String]), lbsParam: LBSParameters): (Long, scala.collection.mutable.Map[Int, String]) = {

    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    var strategy = top._2;
    while (!isGLeafNode(strategy)) {
      val riskValue = getPiOfG(strategy) * getPublishersLoss(strategy);
      if (riskValue <= lbsParam.getRecordCost()) {
        return (top._1, strategy);
      }
      var currentBenefit = getPublishersBenefit(strategy) - riskValue;
      var currentStrategy = strategy;
      val children = getChildren(strategy);
      for (child <- children) {
        val childRiskValue = getPiOfG(child) * getPublishersLoss(child);
        val childBenefit = getPublishersBenefit(child) - childRiskValue;
        if (childBenefit >= currentBenefit) {
          currentStrategy = child;
          currentBenefit = childBenefit;

        }
      }
      strategy = currentStrategy;
    }
    return (top._1, strategy);
  }
}