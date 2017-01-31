package edu.utd.security.lbs

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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

    if (args.length != 7) {
      println("Program variables expected : <HDFS_Data_file_path> <metadata_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <adversaryAttackCost>")
    } else {

      sc.setLogLevel("ERROR");
      setup(args(0), args(1), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble, args(6).toDouble))
      lbs(args(2));
    }
  }
   
    
  var metadataFilePath: String = null;
  var dataReader: DataReader = null;
  var linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
  var totalDataCount: Long = -1;
  var sc: SparkContext = SparkSession
    .builder.appName("LBS").master("local[4]").getOrCreate().sparkContext;


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
  var lbsParam: LBSParameters = null;
 

  def setup(hdfsFilePath: String, metadataPath: String, lbsParam: LBSParameters) {
    dataReader = new DataReader(sc);
    this.linesRDD = dataReader.readDataFile(hdfsFilePath, true);
    this.linesRDD.cache();
    val metadata = Metadata.getInstance(sc, dataReader, metadataPath);
  }
  def lbs(outputFilePath: String) {

    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    val record = linesRDD.first();
    val linesZipped = linesRDD.map((_._2)).zipWithIndex().map { case (map, index) => (index, map) }.cache();
    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();

    for (i <- 0 to getTotalCount().intValue()-1) {
      val optimalRecord = findOptimalStrategy(linesZipped.lookup(i.longValue())(0), new LBSParameters(4, 1200, 2000, 10))
      println("Optimal Strategy  Payoff:" + optimalRecord._1);
      totalPublisherPayOff += optimalRecord._1;
      totalAdvBenefit += optimalRecord._2;
      val arr = optimalRecord._3.toArray
      println(optimalRecord._1+"_"+optimalRecord._2);
      list += ((i, arr.sortBy(_._1).map(_._2).mkString(",")));
      if (i % 3000 == 2999) {
        val vl = sc.parallelize(list.toList)
        rdds = rdds :+ vl;
        list = ListBuffer();
      }
    }
    val vl = sc.parallelize(list.toList)
    rdds = rdds :+ vl;
    new DataWriter(sc).writeRDDToAFile(outputFilePath, sc.union(rdds).sortBy(_._1).map(_._2));

    println("Avg PublisherPayOff found: " + (totalPublisherPayOff / getTotalCount()))
    println("Avg AdversaryBenefit found: " + (totalAdvBenefit / getTotalCount()))
  }

  def getPublishersBenefit(g: scala.collection.mutable.Map[Int, String], lbsParams: LBSParameters): Double =
    {
      //println ("Publisher Benefit" +lbsParams.getMaxPublisherBenefit()+"* ( 1.0 - "+getInformationLoss(g) +"/"+getMaximulInformationLoss() +" ="+lbsParams.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g) / getMaximulInformationLoss())) );
      return lbsParams.getMaxPublisherBenefit() * (1.0 - (getInformationLoss(g) / getMaximulInformationLoss()));
    }

  def getInformationLoss(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      var infoLoss: Double = 0;
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
      for (i <- 0 to metadata.value.numColumns() - 1) {
        // println("i" + i);
        if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
          var count: Long = 0;
          val column = metadata.value.getMetadata(i).get;
          val value = g.get(i).get.trim()
          if (column.getColType() == 's') {
            val children = column.getCategory(value);
            if (value != children.value) {
              count = linesRDD.filter({ case (x, y) => { children.childrenString.contains(y.get(i).get) } }).count();
              infoLoss += (-Math.log(1.0 / count));
            }
          } else {
            val minMax = LBSUtil.getMinMax(value);

            if (minMax._1 != minMax._2) {
              if ((minMax._1 == column.getMin() && (minMax._2 == column.getMax()))) {

                count = getTotalCount();
                infoLoss += (-Math.log(1.0 / count));

              } else {
                count = linesRDD.filter({ case (x, y) => { (y.get(i).get.toDouble >= minMax._1 && y.get(i).get.toDouble <= minMax._2) } }).count();
                infoLoss += (-Math.log(1.0 / count));
              }
            }
          }
        }
      }
      return infoLoss;
    }

  var maximumInfoLoss: Double = -1;

  def getTotalCount(): Long = {
    if (totalDataCount == -1) {
      totalDataCount = linesRDD.count();
    }
    return totalDataCount;
  }

  def getMaximulInformationLoss(): Double =
    {
      if (maximumInfoLoss == -1) {
        val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
        for (i <- 0 to metadata.value.numColumns() - 1) {

          if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
            //println(i + ":" + Math.log(1.0 / metadata.value.getMetadata(i).get.depth()));
            maximumInfoLoss += (-Math.log(1.0 / getTotalCount()));
          }
        }
      }
      return maximumInfoLoss;
    }

  /**
   * Returns probability of adversaries success. Depends on total number of entries that fall in the same category.
   * Should return a number between 0 and 1 - 1 when only single record (self) exists.
   */
  def getRiskOfStrategy(g: scala.collection.mutable.Map[Int, String]): Double =
    {
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
      //println("Calling filter")
      val matchingPopulationGroupSize = linesRDD.filter({
        case (x: Long, y) =>
          isRecordASuperSetOfRecordB(g, y)
      }).count();
      for (i <- 0 to metadata.value.numColumns() - 1) {
        if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
          //print(g.get(i) + ",")
        }
      }
      //println("Risk of Strategy: " + matchingPopulationGroupSize+ " | "+(1.0 / matchingPopulationGroupSize))
      return (1.0 / matchingPopulationGroupSize);
    }

  /**
   * This method checks whether record B is equal to or subset of record A with respect to Quasi-Identifiers.
   */
  def isRecordASuperSetOfRecordB(a: scala.collection.mutable.Map[Int, String], b: scala.collection.mutable.Map[Int, String]): Boolean = {
    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    //println("isRecordASuperSetOfRecordB called")
    for (i <- 0 to metadata.value.numColumns() - 1) {
      if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
        val column = metadata.value.getMetadata(i).get;
        val value1 = a.get(i).get.trim()
        val value2 = b.get(i).get.trim()

        if (!(value1 == value2)) {
          //println(value1 + " | " + value2)
          if (column.getColType() == 's') {
            val children = column.getCategory(value1).childrenString
            //println("Checking " + value2 + " in ->" + value1 + "_" + children + " : " + children.contains(value2))
            if (!children.contains(value2)) {
              //println("returning false")
              return false;
            }
          } else {
            val minMax1 = LBSUtil.getMinMax(value1);
            val minMax2 = LBSUtil.getMinMax(value2);
            //println("Checking " + value1 + " in ->" + value2)
            if (minMax2._1 < minMax1._1 || minMax2._2 > minMax1._2) {
              return false
            }
          }
        }
      }
    }
    return true;
  }

  def findOptimalStrategy(top: scala.collection.mutable.Map[Int, String], lbsParam: LBSParameters): (Double, Double, scala.collection.mutable.Map[Int, String]) = {
    //println("starting ::+:")

    var publisherPayOff: Double = -1;
    var adversaryBenefit: Double = -1;

    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    var genStrategy = top;
    //   println("starting search")
    while (!isGLeafNode(genStrategy)) {
      //println(":0::")
      adversaryBenefit = getRiskOfStrategy(genStrategy) * lbsParam.getPublishersLossOnIdentification(); // adversaryBenefit = publisherLoss.

      // println(":1::")
      publisherPayOff = getPublishersBenefit(genStrategy, lbsParam) - adversaryBenefit;

      //println("::2:("+publisherPayOff+")")
      if (adversaryBenefit <= lbsParam.getRecordCost()) {
        //   println("InnerMost return payoff" + publisherPayOff);
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      // println("Publisher Payoff " + publisherPayOff + ": " + genStrategy);
      var currentStrategy = genStrategy;
      val children = getChildren(genStrategy);

      for (child <- children) {
        //println("children:::")
        val childAdvBenefit = getRiskOfStrategy(child) * lbsParam.getPublishersLossOnIdentification();
        //    println ("childAdvBenefit"+childAdvBenefit);
        val childPublisherPayoff = getPublishersBenefit(child, lbsParam) - childAdvBenefit;
        //println("Child payoff " + childPublisherPayoff + "->" +  "|"+(childPublisherPayoff >= publisherPayOff)+"___"+child)

        if (childPublisherPayoff >= publisherPayOff) {
          //println("Assigning values " + childPublisherPayoff + "->" + child)
          currentStrategy = child;
          adversaryBenefit = childAdvBenefit;
          publisherPayOff = childPublisherPayoff;
        }
      }
      if (currentStrategy == genStrategy) {
        //  println("Parent Payoff is better than any of the children payoff" + publisherPayOff);
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      genStrategy = currentStrategy;
    }
    //  println("Outside return payoff" + publisherPayOff);
    return (publisherPayOff, adversaryBenefit, genStrategy);
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
          val parent = column.getParentCategory(value).value();
          if (parent != value) {
            //println(value +" : "+parent) 
            copyOfG.put(i, column.getParentCategory(value).value().trim());
            list += copyOfG;
          }
        }
      }
      return list.toList;
    }

}