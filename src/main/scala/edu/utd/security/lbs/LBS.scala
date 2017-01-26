package edu.utd.security.lbs

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import edu.utd.security.lbs.LBSUtil

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
  var totalDataCount: Long = -1;
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
    val optimalRecord = findOptimalStrategy(record, new LBSParameters(4, 1200, 2000, 10))
  }

  def getPublishersBenefit(g: scala.collection.mutable.Map[Int, String], lbsParams: LBSParameters): Double =
    {
      return lbsParams.getMaxPublisherBenefit() * (1 - (getInformationLoss(g) / getMaximulInformationLoss(g)));
    }

  def getInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
      for (i <- 0 to metadata.value.numColumns() - 1) {
        if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
          var count: Long = 0;
          val column = metadata.value.getMetadata(i).get;
          val value = g.get(i).get.trim()
          if (column.getColType() == 's') {
            val children = column.getCategory(value).childrenString
            count = linesRDD.filter({ case (x, y) => { children.contains(y.get(i).get) } }).count();
          } else {
            val minMax =LBSUtil.getMinMax(value);
            if ((minMax._1 == column.getMin() && (minMax._2 == column.getMax()))) {
              if (totalDataCount == -1) {
                totalDataCount = linesRDD.count();
              }
              count = totalDataCount;
            } else {
              count = linesRDD.filter({ case (x, y) => { !(y.get(i).get.toDouble > minMax._2 && y.get(i).get.toDouble < minMax._1) } }).count();
            }
          }

          infoLoss += -(1 / Math.log(count));
        }
      }
      return 1;
    }

  var maximumInfoLoss: Double = -1;

  def getMaximulInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      if (maximumInfoLoss == -1) {
        val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
        for (i <- 0 to metadata.value.numColumns() - 1) {

          if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
            maximumInfoLoss += -(1 / Math.log(metadata.value.getMetadata(i).get.depth()));
          }
        }
      }
      return maximumInfoLoss;
    }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   */
  def getRiskOfStrategy(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
      val matchingPopulationGroupSize = linesRDD.filter({
        case (x: Long, y) =>
          isRecordASuperSetOfRecordB(metadata, g, y)
      }).count() - 1;

      /**Self matching will always happen, hence it should be accounted for.*/
      return (1 / matchingPopulationGroupSize);
    }

  /**
   * This method checks whether record B is equal to or subset of record A with respect to Quasi-Identifiers.
   */
  def isRecordASuperSetOfRecordB(metadata: Broadcast[Metadata], a: scala.collection.mutable.Map[Int, String], b: scala.collection.mutable.Map[Int, String]): Boolean = {
    var allAttributesMatch: Boolean = true;
    for (i <- 0 to metadata.value.numColumns() - 1) {
      if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
        val column = metadata.value.getMetadata(i).get;
        val value1 = a.get(i).get.trim()
        val value2 = a.get(i).get.trim().toDouble

        if (column.getColType() == 's') {
          val children = column.getCategory(value1).childrenString
          if (!children.contains(value2)) {
            return false;
          }
        } else {
          val minMax = LBSUtil.getMinMax(value1);
          if (value2 > minMax._2 || value2 < minMax._1) {
            return false
          }
        }
      }
    }
    allAttributesMatch;
  }
  

  def findOptimalStrategy(top: (Long, scala.collection.mutable.Map[Int, String]), lbsParam: LBSParameters): (Long, scala.collection.mutable.Map[Int, String]) = {

    var publisherPayOff: Double = -1;
    var adversaryBenefit: Double = -1;

    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    var genStrategy = top._2;

    while (!isGLeafNode(genStrategy)) {
      adversaryBenefit = getRiskOfStrategy(genStrategy) * lbsParam.getPublishersLossOnIdentification(); // adversaryBenefit = publisherLoss.
      if (adversaryBenefit <= lbsParam.getRecordCost()) {
        return (top._1, genStrategy);
      }
      publisherPayOff = getPublishersBenefit(genStrategy, lbsParam) - adversaryBenefit;
      var currentStrategy = genStrategy;
      val children = getChildren(genStrategy);

      for (child <- children) {
        val childAdvBenefit = getRiskOfStrategy(child) * lbsParam.getPublishersLossOnIdentification();
        val childPublisherPayoff = getPublishersBenefit(child, lbsParam) - childAdvBenefit;
        if (childPublisherPayoff >= publisherPayOff) {
          currentStrategy = child;
          publisherPayOff = childPublisherPayoff;
        }
      }
      genStrategy = currentStrategy;
    }
    return (top._1, genStrategy);
  }

  /**
   * This method returns true of input map corresponds to the bottommost level in lattice.
   */
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

  /**
   * This method returns the list of immediate children from lattice for the given entry.
   */
  def getChildren(g: scala.collection.mutable.Map[Int, String]): List[scala.collection.mutable.Map[Int, String]] =
    {
      /**
       * Iterate over each attribute, generalize the value one step up at a time, accumulate and return the list.
       */
      val list = ListBuffer[scala.collection.mutable.Map[Int, String]]();
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);

      for (i <- 0 to metadata.value.numColumns() - 1) {
        /**
         * Create child for lattice on each column one at a time.
         */

        if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
          var copyOfG = g.clone();
          val column = metadata.value.getMetadata(i).get;
          val value = g.get(i).get.trim()
          copyOfG.put(i, column.getParentCategory(value).value());
          list += copyOfG;
        }
      }
      return list.toList;
    }

}