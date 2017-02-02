package edu.utd.security.risk

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._

import edu.utd.security.mondrian.DataWriter
import scala.io.Source

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
object LBS {

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      println("Program variables expected : <HDFS_Data_file_path> <PREDICT_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <adversaryAttackCost>")
    } else {
      sc.setLogLevel("ERROR");
      setup(args(0), args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble, args(6).toDouble))
    }
  }
  val sc = SparkSession
    .builder.appName("LBS").master("spark://cloudmaster3:7077").getOrCreate().sparkContext;
  def getSC(): SparkContext =
    {
      sc
    }

  /**
   * Using following singleton to retrieve/broadcast metadata variables.
   */
  object Metadata {
    @volatile private var metadata: Broadcast[Metadata] = null;
    @volatile private var totalDataCount: Broadcast[Long] = null;
    def getInstance(sc: SparkContext): Broadcast[Metadata] = {
      if (metadata == null) {
        synchronized {
          if (metadata == null) {
            val data = Source.fromFile("/home/kanchan/workspace2/blueray/src/test/resources/metadata_lbs.xml").getLines().mkString("\n");
            val metadataVal = new DataReader(sc).readMetadata(data);
            metadata = sc.broadcast(metadataVal)
          }
        }
      }
      metadata
    }
    def getTotalCount(sc: SparkContext, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): Broadcast[Long] = {
      if (totalDataCount == null) {
        val totalDataCountVal = linesRDD.count();
        totalDataCount = sc.broadcast(totalDataCountVal)

      }
      return totalDataCount;
    }

    var maximumInfoLoss: Broadcast[Double] = null;

    def getMaximulInformationLoss(sc: SparkContext, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): Broadcast[Double] =
      {
        if (maximumInfoLoss == null) {
          val metadata = getInstance(sc);
          var maximumInfoLossValue: Double = 0.0;
          for (column <- metadata.value.getQuasiColumns()) {
            //println(i + ":" + Math.log(1.0 / metadata.value.getMetadata(i).get.depth()));
            maximumInfoLossValue += (-Math.log(1.0 / Metadata.getTotalCount(sc, linesRDD).value));
          }
          maximumInfoLoss = sc.broadcast(maximumInfoLossValue);
        }
        return maximumInfoLoss;
      }
  }

  def setup(hdfsFilePath: String, predictionFilePath: String, outputFilePath: String, lbsParam: LBSParameters) {
    val linesRDD = new DataReader(getSC()).readDataFile(hdfsFilePath, true).cache();

    val metadata = Metadata.getInstance(getSC());
    lbs(outputFilePath, linesRDD);
  }
  def lbs(outputFilePath: String, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]) {

    val metadata = Metadata.getInstance(getSC());
    val linesZipped = linesRDD.map((_._2)).zipWithIndex().map { case (map, index) => (index, map) }.cache();
    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();

    for (i <- 0 to Metadata.getTotalCount(getSC(), linesRDD).value.intValue() - 1) {
      val optimalRecord = findOptimalStrategy(linesZipped.lookup(i.longValue())(0), new LBSParameters(4, 1200, 2000, 10), linesRDD)
      totalPublisherPayOff += optimalRecord._1;
      totalAdvBenefit += optimalRecord._2;
      val arr = optimalRecord._3.toArray
      println((i + 1) + " " + optimalRecord._1 + "_" + optimalRecord._2);
      // println(arr.sortBy(_._1).map(_._2).mkString(","));
      list += ((i, arr.sortBy(_._1).map(_._2).mkString(",")));
      if (i % 3000 == 2999) {
        val vl = getSC().parallelize(list.toList)
        rdds = rdds :+ vl;
        list = ListBuffer();
      }
    }
    val vl = getSC().parallelize(list.toList)
    rdds = rdds :+ vl;
    new DataWriter(getSC()).writeRDDToAFile(outputFilePath, getSC().union(rdds).sortBy(_._1).map(_._2));

    println("Avg PublisherPayOff found: " + (totalPublisherPayOff / Metadata.getTotalCount(getSC(), linesRDD).value))
    println("Avg AdversaryBenefit found: " + (totalAdvBenefit / Metadata.getTotalCount(getSC(), linesRDD).value))
  }

  def getPublishersBenefit(g: scala.collection.mutable.Map[Int, String], lbsParams: LBSParameters, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): Double =
    {
      //println ("Publisher Benefit" +lbsParams.getMaxPublisherBenefit()+"* ( 1.0 - "+getInformationLoss(g,linesRDD) +"/"+Metadata.getMaximulInformationLoss(getSC(), linesRDD).value +" ="+lbsParams.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g,  linesRDD) / Metadata.getMaximulInformationLoss(getSC(), linesRDD).value)));
      return lbsParams.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g, linesRDD) / Metadata.getMaximulInformationLoss(getSC(), linesRDD).value));
    }

  def getInformationLoss(g: scala.collection.mutable.Map[Int, String], linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): Double =
    {
      var infoLoss: Double = 0;
      val metadata = Metadata.getInstance(getSC());
      for (column <- metadata.value.getQuasiColumns()) {
        var count: Long = 0;
        val value = g.get(column.getIndex()).get.trim()
        if (column.getColType() == 's') {
          val children = column.getCategory(value);
          if (value != children.value) {
            count = linesRDD.filter({ case (x, y) => { children.childrenString.contains(y.get(column.getIndex()).get) } }).count();
            infoLoss += (-Math.log(1.0 / count));
          }
        } else {
          val minMax = LBSUtil.getMinMax(value);

          if (minMax._1 != minMax._2) {
            if ((minMax._1 == column.getMin() && (minMax._2 == column.getMax()))) {

              count = Metadata.getTotalCount(getSC(), linesRDD).value;
              infoLoss += (-Math.log(1.0 / count));

            } else {
              count = linesRDD.filter({ case (x, y) => { (y.get(column.getIndex()).get.toDouble >= minMax._1 && y.get(column.getIndex()).get.toDouble <= minMax._2) } }).count();
              infoLoss += (-Math.log(1.0 / count));
            }
          }
        }
      }
      return infoLoss;
    }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   * Should return a number between 0 and 1 - 1 when only single record (self) exists.
   */
  def getRiskOfStrategy(a: scala.collection.mutable.Map[Int, String], metadata: Metadata, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): Double =
    {
      val matchingPopulationGroupSize = linesRDD.filter({
        case (x: Long, b) => {
          /**
           * This method checks whether record B is equal to or subset of record A with respect to Quasi-Identifiers.
           */
          var status = true;
          breakable {
            for (column <- metadata.getQuasiColumns()) {
              val value1 = a.get(column.getIndex()).get.trim()
              val value2 = b.get(column.getIndex()).get.trim()

              if (!(value1 == value2)) {
                if (column.getColType() == 's') {
                  val children = column.getCategory(value1).childrenString
                  //println("Checking " + value2 + " in ->" + value1 + "_" + children + " : " + children.contains(value2))
                  if (!children.contains(value2)) {
                    //println("returning false")
                    status = false;
                    break;
                  }
                } else {
                  val minMax1 = LBSUtil.getMinMax(value1);
                  val minMax2 = LBSUtil.getMinMax(value2);
                  //println("Checking " + value1 + " in ->" + value2)
                  if (minMax2._1 < minMax1._1 || minMax2._2 > minMax1._2) {
                    status = false;
                    break;
                  }
                }
              }
            }
          }
          status
        }
      }).count();

      //      println("Risk of Strategy: " + matchingPopulationGroupSize + " | " + (1.0 / matchingPopulationGroupSize))
      return (1.0 / matchingPopulationGroupSize);
    }

  def findOptimalStrategy(top: scala.collection.mutable.Map[Int, String], lbsParam: LBSParameters, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): (Double, Double, scala.collection.mutable.Map[Int, String]) = {
    //println("starting ::+:")

    var publisherPayOff: Double = -1;
    var adversaryBenefit: Double = -1;

    val metadata = Metadata.getInstance(getSC()).value;
    var genStrategy = top;
    //   println("starting search")
    while (!isGLeafNode(genStrategy)) {
      //println(":0::")
      adversaryBenefit = getRiskOfStrategy(genStrategy, metadata, linesRDD) * lbsParam.getPublishersLossOnIdentification(); // adversaryBenefit = publisherLoss.

      // println(":1::")
      publisherPayOff = getPublishersBenefit(genStrategy, lbsParam, linesRDD) - adversaryBenefit;

      //println("::2:("+publisherPayOff+")")
      if (adversaryBenefit <= lbsParam.getRecordCost()) {
        //println("InnerMost return payoff" + publisherPayOff);
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      // println("Publisher Payoff " + publisherPayOff + ": " + genStrategy);
      var currentStrategy = genStrategy;
      val children = getChildren(genStrategy);

      for (child <- children) {
        //println("children:::")
        val childAdvBenefit = getRiskOfStrategy(child, metadata, linesRDD) * lbsParam.getPublishersLossOnIdentification();
        //    println ("childAdvBenefit"+childAdvBenefit);
        val childPublisherPayoff = getPublishersBenefit(child, lbsParam, linesRDD) - childAdvBenefit;
        //println("Child payoff " + childPublisherPayoff + "->" +  "|"+(childPublisherPayoff >= publisherPayOff)+"___"+child)

        if (childPublisherPayoff >= publisherPayOff) {
          //println("Assigning values " + childPublisherPayoff + "->" + child)
          currentStrategy = child;
          adversaryBenefit = childAdvBenefit;
          publisherPayOff = childPublisherPayoff;
        }
      }
      if (currentStrategy == genStrategy) {
        //println("Parent Payoff is better than any of the children payoff" + publisherPayOff);
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      genStrategy = currentStrategy;
    }
    //println("Outside return payoff" + publisherPayOff);
    return (publisherPayOff, adversaryBenefit, genStrategy);
  }

  /**
   * This method returns true of input map corresponds to the bottommost level in lattice.
   */
  def isGLeafNode(map: scala.collection.mutable.Map[Int, String]): Boolean =
    {
      val metadata = Metadata.getInstance(getSC());
      for (column <- metadata.value.getQuasiColumns()) {
        val value = map.get(column.getIndex()).get.trim()
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
      val metadata = Metadata.getInstance(getSC());

      /**
       * Create child for lattice on each column one at a time.
       */

      for (column <- metadata.value.getQuasiColumns()) {
        var copyOfG = g.clone();
        val value = g.get(column.getIndex()).get.trim()
        val parent = column.getParentCategory(value).value();
        if (parent != value) {
          //println(value +" : "+parent) 
          copyOfG.put(column.getIndex(), column.getParentCategory(value).value().trim());
          list += copyOfG;
        }
      }
      return list.toList;
    }

}