package edu.utd.security.risk

import scala.annotation.varargs
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.runtime.universe
import scala.util.control.Breaks._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
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
      sc.setLogLevel("ERROR");
      setup(args(0), args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble, args(6).toDouble), args(7).toBoolean, args(8).toInt)
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
            val data = Source.fromFile("/data/kanchan/metadata.xml").getLines().mkString("\n");
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
            /*val denom = Metadata.getTotalCount(sc, linesRDD).value*/
            //println("NumUnique: " + column.getName() + "::" + column.getNumUnique())
            maximumInfoLossValue += (-Math.log(1.0 / column.getNumUnique()));
          }
          maximumInfoLoss = sc.broadcast(maximumInfoLossValue);
        }
        return maximumInfoLoss;
      }
  }

  def setup(hdfsFilePath: String, predictionFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: Boolean, numNeighbours: Int) {
    val linesRDD = new DataReader(getSC()).readDataFile(hdfsFilePath, true).cache();

    val metadata = Metadata.getInstance(getSC());
    lbs(outputFilePath, linesRDD, useLSH, lbsParam, numNeighbours);
  }
  def lbs(outputFilePath: String, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], useLSH: Boolean, lbsParam: LBSParameters, numNeighbours: Int) {

    val metadata = Metadata.getInstance(getSC());
    val linesZipped = linesRDD.map((_._2)).zipWithIndex().map { case (map, index) => (index, map) }.cache();
    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH) {
      val output = lsh(linesRDD, lbsParam, numNeighbours)
      rdds = output._3
      totalPublisherPayOff = output._1
      totalAdvBenefit = output._2
    } else {
      for (i <- 0 to Metadata.getTotalCount(getSC(), linesRDD).value.intValue() - 1) {
        val optimalRecord = findOptimalStrategy(linesZipped.lookup(i.longValue())(0), lbsParam, linesRDD)
        totalPublisherPayOff += optimalRecord._1;
        totalAdvBenefit += optimalRecord._2;
        val arr = optimalRecord._3.toArray
        println((i + 1) + " " + optimalRecord._1 + "_" + optimalRecord._2);
        println(arr.sortBy(_._1).map(_._2).mkString(","));
        list += ((i, arr.sortBy(_._1).map(_._2).mkString(",")));
        if (i % 3000 == 2999) {
          val vl = getSC().parallelize(list.toList)
          rdds = rdds :+ vl;
          list = ListBuffer();
        }
      }
    }
    val vl = getSC().parallelize(list.toList)
    rdds = rdds :+ vl;
    new DataWriter(getSC()).writeRDDToAFile(outputFilePath, getSC().union(rdds).sortBy(_._1).map(_._2));

    println("Avg PublisherPayOff found: " + (totalPublisherPayOff / Metadata.getTotalCount(getSC(), linesRDD).value))
    println("Avg AdversaryBenefit found: " + (totalAdvBenefit / Metadata.getTotalCount(getSC(), linesRDD).value))
  }

  def lsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int): (Double, Double, List[RDD[(Int, String)]]) = {
    val metadata = Metadata.getInstance(getSC());
    val quasiRows = LBSUtil.getMinimalDataSet(metadata.value, linesRDD, false);

    val linesZipped = linesRDD.map((_._2)).zipWithIndex().map { case (map, index) => (index, map) }.cache();
        val columnStartCounts = LBSUtil.getColumnStartCounts(metadata.value);
    println(quasiRows.take(1).mkString("]]"));
    val inputToModel = quasiRows.map({
      case (x, y) => ({
        val row = LBSUtil.extractRow(metadata.value, columnStartCounts, y, true)
        (x.intValue(), Vectors.dense(row))
      })
    }).collect().toSeq

    val dataFrame = new SQLContext(sc).createDataFrame(inputToModel).toDF("id", "keys");
    val key = Vectors.dense(1.0, 0.0)
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(dataFrame)
    val txModel = model.transform(dataFrame)


    var mapOfIdAndPredict = collection.mutable.Map((linesRDD.map({ case (x, y) => (x, true) }).collect().toMap.toSeq: _*));

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;
    val nonQuasiRows = LBSUtil.getMinimalDataSet(metadata.value, linesRDD, true).zipWithIndex().map { case (map, index) => (index, map) }.cache(); ;
    var counter = 0;
    var predicted = 0;
    val keys = mapOfIdAndPredict.keys.toArray;
    for (key <- keys) {
      val value = mapOfIdAndPredict.get(key);
      if (value.get == true) {
        val currentRecord =linesZipped.lookup(key)(0);
        val optimalRecord = findOptimalStrategy(currentRecord, lbsParam, linesRDD)
        predicted = predicted + 1;
        val neighbors = model.approxNearestNeighbors(txModel, Vectors.dense(LBSUtil.extractRow(metadata.value, columnStartCounts, quasiRows.lookup(key)(0), true)), numNeighbors).collectAsList().asInstanceOf[java.util.List[Row]];
        var quasiGeneralizedMap = scala.collection.mutable.Map[Int, String]();

        for (column <- metadata.value.getQuasiColumns()) {
          quasiGeneralizedMap.put(column.getIndex(), optimalRecord._3.get(column.getIndex()).get.trim());
        }
        println(key +" =>"+(counter+predicted) +"-->"+quasiGeneralizedMap.mkString(",") )
     
        for (i <- 0 to neighbors.size() - 1) {
          val neighbor = neighbors.get(i);
          
          val nebkey =neighbor.get(0).asInstanceOf[Int].longValue()
          
          if (mapOfIdAndPredict.get(nebkey).get) {
          val output = LBSUtil.extractReturnObject(metadata.value, columnStartCounts, (neighbor.get(1).asInstanceOf[DenseVector]).values);

            var neighborIsSubSet = true;

            for (column <- metadata.value.getQuasiColumns()) {
              if (neighborIsSubSet) {

                val genHierarchyValue = optimalRecord._3.get(column.getIndex()).get.trim()
                val neighborValue = output.get(column.getIndex()).get.trim();

                if (genHierarchyValue != neighborValue) {
                  if (column.getColType() == 's') {
                    val genCategory = column.getCategory(genHierarchyValue);
                    if (!genCategory.children.contains(neighborValue)) {
                      neighborIsSubSet = false;
                    }
                  } else {
                    val minMax1 = LBSUtil.getMinMax(genHierarchyValue);
                    val minMax2 = LBSUtil.getMinMax(neighborValue);
                    //println("Mismatched :  " + minMax1 + " " + minMax2)
                    if (minMax1._1 > minMax2._1 || minMax1._2 < minMax2._2) {
                      neighborIsSubSet = false;
                    }
                  }
                }
              }
            }
            if (neighborIsSubSet) {
              counter = counter + 1;
              mapOfIdAndPredict.put(neighbor.get(0).asInstanceOf[Int].longValue(), false);
              totalPublisherPayOff += optimalRecord._1;
              totalAdvBenefit += optimalRecord._2;
             // print(nebkey+" " )
              val arr = optimalRecord._3.toArray
           //   println(neighbor.get(0) + ": " + optimalRecord._1 + "_" + optimalRecord._2);
              val nonQuasiMap = nonQuasiRows.lookup(neighbor.get(0).asInstanceOf[Int].longValue())(0)._2
              val outputMap = (nonQuasiMap ++= quasiGeneralizedMap).toArray.sortBy(_._1).map(_._2);
            //  println(outputMap.mkString(","));
              list += ((neighbor.get(0).asInstanceOf[Int], outputMap.mkString(",")));
              if (i % 3000 == 2999) {
                val vl = getSC().parallelize(list.toList)
                rdds = rdds :+ vl;
                list = ListBuffer();
              }
            }
          } 
        }   
        println();
        if(mapOfIdAndPredict.get(key).get)
        {
        mapOfIdAndPredict.put(key, false);
        totalPublisherPayOff += optimalRecord._1;
        totalAdvBenefit += optimalRecord._2;
        val arr = optimalRecord._3.toArray
        val nonQuasiMap = nonQuasiRows.lookup(key)(0)._2
        val outputMap = (nonQuasiMap ++= quasiGeneralizedMap).toArray.sortBy(_._1).map(_._2);
        list += ((key.intValue(), outputMap.mkString(",")));
        }
        else
        {
              counter = counter - 1;
          
        }
      }  
    }
    
    val vl = getSC().parallelize(list.toList)
    rdds = rdds :+ vl;
    println("Number of predictions done :" + predicted + " : neighbours: " + counter);
    return (totalPublisherPayOff, totalAdvBenefit, rdds);
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
          if (children.leaves.length != 0) {
            //   println(value + " _ " + (-Math.log(1.0 / children.leaves.length)));
            infoLoss += (-Math.log(1.0 / children.leaves.length));
          }
          /*if (value != children.value) {
            count = linesRDD.filter({ case (x, y) => { children.childrenString.contains(y.get(column.getIndex()).get) } }).count();
            infoLoss += (-Math.log(1.0 / count));
          }*/
        } else {
          val minMax = LBSUtil.getMinMax(value);
          if (minMax._1 != minMax._2) {
            // println(value + " _ " + (-Math.log(1.0 / (1 + minMax._2 - minMax._1))))
            infoLoss += (-Math.log(1.0 / (1 + minMax._2 - minMax._1)));
          }
          /*if (minMax._1 != minMax._2) {
            if ((minMax._1 == column.getMin() && (minMax._2 == column.getMax()))) {

              count = Metadata.getTotalCount(getSC(), linesRDD).value;
              infoLoss += (-Math.log(1.0 / count));

            } else {
              count = linesRDD.filter({ case (x, y) => { (y.get(column.getIndex()).get.toDouble >= minMax._1 && y.get(column.getIndex()).get.toDouble <= minMax._2) } }).count();
              infoLoss += (-Math.log(1.0 / count));
            }
          }*/
        }
      }
      //   println("Total infoLoss for " + g + " =" + infoLoss);
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

      //println("Risk of Strategy: " + matchingPopulationGroupSize + " | " + (1.0 / matchingPopulationGroupSize))
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

      //println(":1::")
      publisherPayOff = getPublishersBenefit(genStrategy, lbsParam, linesRDD) - adversaryBenefit;

      //  println("::2:("+publisherPayOff+")")
      if (adversaryBenefit <= lbsParam.getRecordCost()) {
        return (publisherPayOff, adversaryBenefit, genStrategy);
      }
      // println("Publisher Payoff " + publisherPayOff + ": " + genStrategy);
      var currentStrategy = genStrategy;
      val children = getChildren(genStrategy);

      for (child <- children) {
        //println("children:::"+child)
        val childAdvBenefit = getRiskOfStrategy(child, metadata, linesRDD) * lbsParam.getPublishersLossOnIdentification();
        //    println ("childAdvBenefit"+childAdvBenefit);
        val childPublisherPayoff = getPublishersBenefit(child, lbsParam, linesRDD) - childAdvBenefit;
        //println("Child payoff " + childPublisherPayoff + "->" +  "|"+(childPublisherPayoff >= publisherPayOff)+"___"+child)

        if (childPublisherPayoff >= publisherPayOff) {
          //    println("Assigning values " + childPublisherPayoff + "->" + child)
          currentStrategy = child;
          adversaryBenefit = childAdvBenefit;
          publisherPayOff = childPublisherPayoff;
        }
      }
      if (currentStrategy == genStrategy) {
        //println("Selected "+currentStrategy);
        // println("Parent Payoff is better than any of the children payoff" + publisherPayOff);
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