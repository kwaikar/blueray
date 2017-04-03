package edu.utd.security.risk

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe
import scala.util.Random
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import edu.utd.security.mondrian.DataWriter
import breeze.linalg.normalize
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._

import scala.util.Random

import breeze.linalg.normalize
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{ Experimental, Since }
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * This is implementation of paper called "A Game Theoretic Framework for Analyzing Re-Identification Risk"
 * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
 * booktitle = {In ICDE},
 * year = {2015}
 */
object LBSAndLSH {

  var sc: SparkContext = null;

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("Program variables expected : <SPARK_MASTER> <HDFS_Data_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <numPartitions> <Algorithm(lbs/lbslsh/lsh)> <LSH_NUM_NEIGHBORS>")
    } else {

      sc = SparkSession
        .builder.appName("LBS").master(args(0)).getOrCreate().sparkContext;
      sc.setLogLevel("ERROR");
      setup(args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7), args(8).toInt, args(6).toInt)

    }
  }

  def setup(hdfsFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: String, numNeighbours: Int, numParitions: Int) {
    var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, numParitions).cache();
    lbs(outputFilePath, linesRDD, useLSH, lbsParam, numNeighbours, numParitions);
  }
  def lbs(outputFilePath: String, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], useLSH: String, lbsParam: LBSParameters, numNeighbours: Int, numPartitons: Int) {

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH.equalsIgnoreCase("lsh")) {
      val output = lsh(linesRDD, lbsParam, numNeighbours, outputFilePath, numPartitons)
    } else if (useLSH.equalsIgnoreCase("lbslsh")) {
      lbslsh(linesRDD, lbsParam, numNeighbours, outputFilePath);
    } else {

      val t0 = System.nanoTime()
      val metadata = LBSMetadataWithSparkContext.getInstance(sc);
      val zips = LBSMetadataWithSparkContext.getZip(sc);
      val population = LBSMetadataWithSparkContext.getPopulation(sc);
      val map: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]();
      map.put(0, "*");
      map.put(3, "*");
      map.put(2, "48");
      map.put(1, "38363");
      /* println("NumMatches" + new LBSAlgorithmWithSparkContext(zips, population, metadata.value, lbsParam).getNumMatches(map));
      val i = 29779;
      val algorithm = new LBSAlgorithmWithSparkContext(zips, population, metadata.value, lbsParam);

      val optimalRecord = algorithm.findOptimalStrategy(linesRDD.lookup(i.longValue())(0));
      println(optimalRecord);
*/
      val output = linesRDD.map({ case (x, y) => (x, new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam).findOptimalStrategy(y)) }).sortByKey().values;
      output.cache();
      val publisherBenefit = output.map({ case (x, y, z) => (x) }).mean();
      val advBenefit = output.map({ case (x, y, z) => (y) }).mean();
      val records = output.map({ case (x, y, z) => (z) });

      println("Avg PublisherPayOff found: " + publisherBenefit)
      println("Avg AdversaryBenefit found: " + advBenefit)
      val fileName = outputFilePath + "/LBS_" + lbsParam.V() + "_" + lbsParam.L() + "_" + lbsParam.C() + ".csv";
      new DataWriter(sc).writeRDDToAFile(fileName, records);
      val t1 = System.nanoTime()
      println("Time Taken: " + ((t1 - t0) / 1000000));
      output.unpersist(true);
    }

  }

  val numHashFunctions: Int = 3;

  val r: Double = 2.5;

  def getRandomUnitVectors(dimensions: Int): ListBuffer[Array[Double]] =
    {
      val rand = new Random();
      val unitVecs: ListBuffer[Array[Double]] = ListBuffer();
      for (i <- 0 to numHashFunctions - 1) {
        {
          val value = Array.fill(dimensions)(rand.nextGaussian());
          val total = Math.sqrt(value.map(x => (x * x)).sum);
          val op = value.map(x => x / total)
          unitVecs.append(op);
        }
      }
      unitVecs;
    }
  def getBuckets(metadata: Broadcast[Metadata], normalizedLinesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], tuneDownFactor: Double): RDD[(Array[(Long, scala.collection.mutable.Map[Int, String])])] =
    {
      val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata.value));
      val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
      val unitVectors = getRandomUnitVectors(columnCounts);
      //    val entriesConverted = sc.longAccumulator("entriesConverted")
      //   val entriesHashed = sc.longAccumulator("entriesHashed")
      println("NUMUnitVectors " + unitVectors.length);
      val buckets = normalizedLinesRDD.map({
        case (x, y) => (
          {
            //       entriesConverted.add(1L);
            (x, y, LSHUtil.extractRow(metadata.value, y))
          })
      }).map({
        case (x, y, mappedY) => {
          var hashValues: java.lang.StringBuilder = new java.lang.StringBuilder();
          var totalSum = 0.0
          for (unitVec <- unitVectors) {
            totalSum = unitVec.zip(mappedY).map({ case (x, y) => x * y }).sum;
            hashValues.append(Math.round((totalSum / r) * tuneDownFactor) / tuneDownFactor + "|")
          }
          //   entriesHashed.add(1);
          (hashValues.toString(), Seq[(Long, scala.collection.mutable.Map[Int, String])]((x, y)))
        }
      }). /*groupByKey().map(_._2.toArray)*/ reduceByKey(_ ++ _).map(x => (x._2.toArray));
      buckets;
    }

  /**
   * This method is used for using LSH bucketing feature for improving performance of LBS algorithm.
   */
  def lbslsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String) = {

    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);

    /**
     * The algorithm starts by specifying precision factor as a high value in order to get minimum number of 
     * false positives.
     */
    var inputData = linesRDD;
    var precisionFactor = 40000;
    /**
     * We hash entire dataset - this should lead to hashing of almost-duplicate entries into same bucket. 
     */
    var buckets = getBuckets(metadata, inputData, precisionFactor);
    buckets.cache();
    val outputs = buckets.flatMap(bucket =>
      {
        /**
         * We loop on each bucket and  
         */
        var list = ListBuffer[(Double, Double, Long, String)]();
        var strategy = new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam).findOriginalOptimalStrategy(bucket(0)._2);
        var stringRepresentation = strategy._3.toArray.sortBy(_._1).map(_._2).mkString(",");
        list.append((strategy._1, strategy._2, bucket(0)._1, stringRepresentation));

        for (i <- 1 to bucket.length - 1) {
          var entry = bucket(i);
          if (isNeighbourSubset(metadata.value, strategy._3, entry._2)) {

            list.append((strategy._1, strategy._2, entry._1, stringRepresentation));
          } else {
            var childStrategy = new LBSAlgorithm(LBSMetadata.getInstance(), lbsParam).findOptimalStrategy(entry._2);
            list.append((childStrategy._1, childStrategy._2, entry._1, childStrategy._3));
          }
        }
        list
      });
    outputs.cache();
    println("Checking pub benefit")
    val publisherBenefit = outputs.map({ case (x, y, z, t) => (x) }).mean();

    println("Checking adv benefit")
    val advBenefit = outputs.map({ case (x, y, z, t) => (y) }).mean();
    val records = outputs.map({ case (x, y, z, t) => (z, t) }).sortByKey().map(_._2);

    println("Avg PublisherPayOff found: " + publisherBenefit)
    println("Avg AdversaryBenefit found: " + advBenefit)

    val fileName = outputFilePath + "/LBSLSH_" + numNeighborsVal.value + "_" + numHashFunctions + "_" + r + ".csv";
    new DataWriter(sc).writeRDDToAFile(fileName, sc.union(rdds).sortByKey().map(_._2));

    val t1 = System.nanoTime()

    println("Time Taken: " + ((t1 - t0) / 1000000));
    outputs.unpersist(true)
    buckets.unpersist(true)
  }

  /**
   * This method implements Strict K-Anonymity using LSH based bucketing technique.
   * The method uses LSH in order to hash the input dataset into buckets, then
   * It computes and assigns summary statistic for the bucket to all items.
   * It then reduces precision factor by  1/10'th of its original value and performs second
   * pass. It continues until precision factor size reaches 1.
   */
  def lsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String, numPartitons: Int) = {
    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);

    var inputData = linesRDD;
    /**
     * WE start with 100000 as precision factor - which means bucket hash-value would be preserved upto 5 decimal places.
     * and hash all elements by calling getBuckets() function.
     */
    var precisionFactor = 100000.0;
    while (precisionFactor > 1) {
      println("NumBuckets: " + precisionFactor + " : ");
      var buckets = getBuckets(metadata, inputData, precisionFactor);
      buckets.cache();
      /**
       * We pickup buckets with size greater than or equal to "k" or number of neighbours
       */
      var neighbours = buckets.filter({ case (y) => y.size >= numNeighborsVal.value });
      /**
       * We assign summary statistic to all elements and then filter these elements.
       */
      var op = neighbours.map(x => (x)).flatMap(x => assignSummaryStatistic(metadata, x));
      var remaining = buckets.filter({ case (y) => y.size < numNeighborsVal.value });
      /**
       * The remaining values are re-grouped in next iteration.
       */
      inputData = remaining.flatMap(x => x)
      rdds.append(op);
      buckets.unpersist(true);

      precisionFactor = precisionFactor / 10;
    }
    val finalStep = inputData.collect();
    println("Final step size" + finalStep.size)
    /**
     * We assign the final summary statistic to all remaining entries.
     */
    var op = sc.parallelize(assignSummaryStatistic(metadata, finalStep).toSeq);
    rdds.append(op);

    val fileName = outputFilePath + "/LSH_" + numNeighborsVal.value + "_" + numHashFunctions + "_" + r + ".csv";
    new DataWriter(sc).writeRDDToAFile(fileName, sc.union(rdds).sortByKey().map(_._2));
    val t1 = System.nanoTime()
    println("Time Taken: " + ((t1 - t0) / 1000000));
    /**
     * Following code is for computation of the Information loss and thus is not included while calculating performance.
     */
    val linesRDDOP = new DataReader().readDataFile(sc, fileName, numPartitons);
    linesRDDOP.cache()
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));
    linesRDDOP.unpersist(true);
  }
  /**
   * This method checks whether generalizedParent is valid parent of "neighbour"
   */
  def isNeighbourSubset(metadata: Metadata, generalizedParent: scala.collection.mutable.Map[Int, String], neighbour: scala.collection.mutable.Map[Int, String]): Boolean =
    {
      var neighborIsSubSet = true;

      breakable {
        /**
         * Here, we loop over all elements and if even one element is not matching, we return false.
         */
        for (column <- metadata.getQuasiColumns()) {

          val genHierarchyValue = generalizedParent.get(column.getIndex()).get.trim()
          val neighborValue = neighbour.get(column.getIndex()).get.trim();

          if (genHierarchyValue != neighborValue) {
            if (column.getColType() == 's') {
              val genCategory = column.getCategory(genHierarchyValue);
              if (!genCategory.children.contains(neighborValue)) {
                neighborIsSubSet = false;
                break;
              }
            } else {
              val minMax1 = LSHUtil.getMinMax(genHierarchyValue);
              val minMax2 = LSHUtil.getMinMax(neighborValue);
              if (minMax1._1 > minMax2._1 || minMax1._2 < minMax2._2) {
                neighborIsSubSet = false;
                break;
              }
            }
          }
        }
      }
      neighborIsSubSet;
    }
    /**
     * This method calculates summary statitic for the Array of lines received. 
     * Assumption is that input dataset contains only attributes of our interest. i.e. quasiIdentifier fields.
     * This assumption was made in order to get accurate statistics of the algorithm.
     */
  def assignSummaryStatistic(metadata: Broadcast[Metadata], lines: Array[(Long, scala.collection.mutable.Map[Int, String])]): Map[Long, String] =
    {
      var indexValueGroupedIntermediate = lines.flatMap({ case (x, y) => y }).groupBy(_._1).map(x => (x._1, x._2.map(_._2)));
      var int2 = indexValueGroupedIntermediate.map({ case (index, list) => (index, list.toList.distinct) })
      var map = indexValueGroupedIntermediate.map({
        case (x, y) =>
          val column = metadata.value.getMetadata(x).get;
          if (column.getColType() == 's') {
            (x, column.findCategory(y.toArray).value());
          } else {
            val listOfNumbers = y.map(_.toDouble);
            if (listOfNumbers.min == listOfNumbers.max) {
              (x, listOfNumbers.min.toString);
            } else {
              (x, listOfNumbers.min + "_" + listOfNumbers.max);
            }
          }
      });
      /**
       * Once we have found the generalization hierarchy,map it to all lines and return the same. 
       */
      val generalization = map.toArray.sortBy(_._1).map(_._2).mkString(",");
      return lines.map({
        case (x, y) =>
          (x, generalization)
      }).toMap;
    }

}