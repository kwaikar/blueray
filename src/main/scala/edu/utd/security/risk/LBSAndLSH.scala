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
import scala.collection.immutable.HashSet

/**
 * This class implements three algorithms described in the thesis
 * LBS algorithm based on Game theoretical approach
 * LSH algorithm based on iterative LSH bucketing technique
 * LBS-LSH algorithm which uses LSH in order to expedite LBS algorithm.
 */
object LBSAndLSH {

  var sc: SparkContext = null;

  var numHashFunctions: Int = 3;
  var r: Double = 3;
  var precisionFactor = 10000.0;
  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("Program variables expected : <SPARK_MASTER> <HDFS_Data_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <numPartitions> <Algorithm(lbs/lbslsh/lsh)> <LSH_NUM_NEIGHBORS>")
    } else {
      sc = SparkSession
        .builder.appName(args(7)).master(args(0)).getOrCreate().sparkContext;
      sc.setLogLevel("ERROR");
      /*val precs = List( 10.0, 100.0,500, 1000.0,5000, 10000.0, 100000.0, 1000000.0, 1000000.0);
      val hashes = List( 3, 4,5,6, 8,9, 10,12,15, 20);
      for (prec <- precs) {
        for (hash <- hashes) {
          println("Running experiment for =>" + prec + " " + hash);
          numHashFunctions = hash;
          precisionFactor = prec*/
      executeAlgorithm(args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7), args(8).toInt, args(6).toInt)
      /*}
      }

*/ }
  }

  def executeAlgorithm(hdfsFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: String, numNeighbours: Int, numPartitons: Int) {

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH.equalsIgnoreCase("lsh")) {
      var linesRDD = new DataReader().readDataFileToNativeFormat(sc, hdfsFilePath, numPartitons);
      lsh(linesRDD, lbsParam, numNeighbours, outputFilePath, numPartitons)
    } else if (useLSH.equalsIgnoreCase("lbslsh")) {
      var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, numPartitons);

      lbslsh(linesRDD, lbsParam, outputFilePath);
    } else {
      /**
       * Execute LBS algorithm based on following paper.
       * Title = "A Game Theoretic Framework for Analyzing Re-Identification Risk"
       * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
       * booktitle = {In ICDE},
       * year = {2015}
       */
      var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, numPartitons);

      val t0 = System.nanoTime()
      val metadata = LBSMetadataWithSparkContext.getInstance(sc);
      val zips = LBSMetadataWithSparkContext.getZip(sc);
      val population = LBSMetadataWithSparkContext.getPopulation(sc);
      val hashedPopulation = LBSMetadataWithSparkContext.getHashedPopulation(sc);
      val params = sc.broadcast(lbsParam);
      /*
       val i = 29779;
      val algorithm = new LBSAlgorithm(metadata.value, lbsParam,population.value,zips.value,hashedPopulation.value);
      val optimalRecord = algorithm.findOptimalStrategy(linesRDD.lookup(i.longValue())(0));
      println(optimalRecord);*/

      /**
       * Here, we calculate the optimal Generalizaiton level for entire RDD.
       */
      val output = linesRDD.mapPartitions(partition =>
        {
          val algo = new LBSAlgorithm(metadata.value, params.value, population.value, zips.value, hashedPopulation.value);
          partition.map {
            case (x, y) => {
              val strategy = algo.findOptimalStrategy(y);
              (x, strategy._1, strategy._2, strategy._3)
            }
          }
        });
      output.persist(StorageLevel.MEMORY_AND_DISK);

      /**
       * We cache RDD as we do not want to be recomputed for each of the following three actions.
       */
      val publisherBenefit = output.map(_._2).mean();
      val advBenefit = output.map(_._3).mean();
      val records = output.sortBy(_._1).map(_._4);

      println("Avg PublisherPayOff found: " + publisherBenefit)
      println("Avg AdversaryBenefit found: " + advBenefit)
      val fileName = outputFilePath + "/LBS_" + lbsParam.V() + "_" + lbsParam.L() + "_" + lbsParam.C() + ".csv";

      new DataWriter(sc).writeRDDToAFile(fileName, records);

      val t1 = System.nanoTime()
      println("Time Taken: " + ((t1 - t0) / 1000000));
    }

  }

  /**
   * This method returns Random Unit vectors
   */
  def getRandomUnitVectors(dimensions: Int): ListBuffer[Array[Double]] =
    {
      val rand = new Random();
      val unitVecs: ListBuffer[Array[Double]] = ListBuffer();
      for (i <- 0 to numHashFunctions - 1) {
        {
          /**
           * Compute Unit vector by picking random entries from Gaussian distribution and then
           * normalizing the vector.
           */
          val value = Array.fill(dimensions)(rand.nextGaussian());
          val total = Math.sqrt(value.map(x => (x * x)).sum);
          val op = value.map(x => x / total)
          unitVecs.append(op);
        }
      }
      unitVecs;
    }
  /**
   * This method implements LSH based Bucket Hashing. Here, we first compute Random unit vector and then multiply
   * the same with the row vector. we divide it by "r" and this becomes the Hash value for the row.
   * We compute this for multiple different hash functions and form the concatenated hash function.
   * Based on the concatenated hash function, elements are grouped and buckets are returned.
   * Formula implemented
   * 		H(x) = Math.round((U.X)/r, precision)
   * 			Where H(x) Hash value
   * 			U -> Random Unit Vector
   * 			X -> Row vector
   * 			r -> Fixed divisor.
   */

  def getBucket(normalizedLinesRDD: RDD[(Long, (String, Int, Int, String), ListBuffer[Double])]): RDD[(Iterable[(Long, (String, Int, Int, String))])] =
    {

      /**
       * For each row, compute the Hash Value.
       */
      val buckets = normalizedLinesRDD.map({
        case (x, y, sum) => {
          (sum.map(x => { Math.round(x * precisionFactor) / precisionFactor }).mkString(","), Seq[(Long, (String, Int, Int, String))]((x, y)));
        }
      }) /*.groupByKey().values;*/ .reduceByKey(_ ++ _).values.map(_.toIterable) /*.map(x => (x._2.toArray));*/ /* Group values by Concatenated Hash key.*/
      buckets;

    }

  def getBucketsNoNormalization(normalizedLinesRDD: RDD[(Long, Map[Int, String], ListBuffer[Double])]): RDD[(Iterable[(Long, Map[Int, String])])] =
    {

      /**
       * For each row, compute the Hash Value.
       */
      val buckets = normalizedLinesRDD.map({
        case (x, y, sum) => {
          (sum.map(x => { Math.round(x * precisionFactor) / precisionFactor }).mkString(","), Seq[(Long, scala.collection.immutable.Map[Int, String])]((x, y.map(z => (z._1, z._2)))));
        }
      }) /*.groupByKey().values;*/ .reduceByKey(_ ++ _).values.map(_.toIterable) /*.map(x => (x._2.toArray));*/ /* Group values by Concatenated Hash key.*/
      buckets;

    }

  /**
   * This method is used for using LSH bucketing feature for improving performance of LBS algorithm.
   */
  def lbslsh(linesRDD: RDD[(Long, scala.collection.immutable.Map[Int, String])], lbsParam: LBSParameters, outputFilePath: String) = {

    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();

    val metadata = LBSMetadataWithSparkContext.getInstance(sc);

    /**
     * The algorithm starts by specifying precision factor as a high value in order to get minimum number of
     * false positives.
     */
    var precisionFactor = 10000;

    /**
     * We hash entire dataset - this should lead to hashing of almost-duplicate entries into same bucket.
     */
    var inputData = getBucketMapping(metadata, linesRDD);
    var buckets = getBucketsNoNormalization(inputData);
    val population = LBSMetadataWithSparkContext.getPopulation(sc);
    val zips = LBSMetadataWithSparkContext.getZip(sc);
    val hashedPopulation = LBSMetadataWithSparkContext.getHashedPopulation(sc);
    val params = sc.broadcast(lbsParam);
    val output = buckets.mapPartitions(partition =>
      {
        val algo = new LBSAlgorithm(metadata.value, params.value, population.value, zips.value, hashedPopulation.value);
        partition.flatMap(bucket =>
          {
            /**
             * We loop on each bucket in order to identify the ideal generalization level.
             * We take first entry, find out generalization level.
             */
            val itr = bucket.iterator;
            var list = ListBuffer[(Long, Double, Double, String)]();
            if (itr.hasNext) {
              var current = itr.next();
              var strategy = algo.findOriginalOptimalStrategy(current._2);
              var stringRepresentation = strategy._3.toArray.sortBy(_._1).map(_._2).mkString(",");
              list.append((current._1, strategy._1, strategy._2, stringRepresentation));
              /**
               * We loop on rest of the entries from the bucket and see if generalization level can be
               * applied or not. If not possible, it computes generalization for the same by invoking LBS algorithm.
               */
              while (itr.hasNext) {
                current = itr.next();
                if (isNeighbourSubset(metadata.value, strategy._3, current._2)) {
                  list.append((current._1, strategy._1, strategy._2, stringRepresentation));
                } else {
                  var childStrategy = algo.findOptimalStrategy(current._2);
                  list.append((current._1, childStrategy._1, childStrategy._2, childStrategy._3));
                }
              }
            }
            list
          });
      });
    output.persist(StorageLevel.MEMORY_AND_DISK);

    val publisherBenefit = output.map(_._2).mean();
    val advBenefit = output.map(_._3).mean();
    val records = output.sortBy(_._1).map(_._4);

    println("Avg PublisherPayOff found: " + publisherBenefit)
    println("Avg AdversaryBenefit found: " + advBenefit)

    val fileName = outputFilePath + "LBSLSH_" + numHashFunctions + "_" + r + ".csv";
    new DataWriter(sc).writeRDDToAFile(fileName, records);

    val t1 = System.nanoTime()

    println("Time Taken: " + ((t1 - t0) / 1000000));

  }
  def getBucketMappingNative(metadata: Broadcast[Metadata], linesRDD: RDD[(Long, (String, Int, Int, String))]): RDD[(Long, (String, Int, Int, String), ListBuffer[Double])] = {

    val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
    val unitVectors = sc.broadcast(getRandomUnitVectors(columnCounts));

    var totalCols = sc.broadcast(LSHUtil.getTotalNewColumns(metadata.value));

    return linesRDD.mapPartitions({
      var nextStartCount = 0;
      val counts = ListBuffer[Int]();
      for (column <- metadata.value.getQuasiColumns()) {
        counts += nextStartCount;
        if (column.isCharColumn()) {
          nextStartCount = nextStartCount + column.getNumUnique();
        } else {
          nextStartCount = nextStartCount + 1;
        }
      }
      val countsArr = counts.toArray
      _.map({
        case (x, (p, q, r, s)) => (x, (p, q, r, s), {
          var index = 0;
          var row = new Array[Double](totalCols.value);

          var column = metadata.value.getMetadata(0).get;
          row((countsArr(0) + column.getRootCategory().getIndexOfColumnValue(p))) = 1.0;
          column = metadata.value.getMetadata(3).get;
          row((countsArr(3) + column.getRootCategory().getIndexOfColumnValue(s))) = 1.0;
          column = metadata.value.getMetadata(1).get;
          row(countsArr(1)) = ((q) - column.getMin()) / (column.getRange());
          column = metadata.value.getMetadata(2).get;
          row(countsArr(2)) = ((r) - column.getMin()) / (column.getRange());

          unitVectors.value.map(unitVect => {
            (unitVect.zip(row).map({ case (x, y) => x * y }).sum) / r
          });
        })
      })
    });
  }

  def getBucketMapping(metadata: Broadcast[Metadata], linesRDD: RDD[(Long, scala.collection.immutable.Map[Int, String])]): RDD[(Long, scala.collection.immutable.Map[Int, String], ListBuffer[Double])] = {

    val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
    val unitVectors = sc.broadcast(getRandomUnitVectors(columnCounts));

    var totalCols = sc.broadcast(LSHUtil.getTotalNewColumns(metadata.value));

    return linesRDD.mapPartitions({
      var nextStartCount = 0;
      val counts = ListBuffer[Int]();
      for (column <- metadata.value.getQuasiColumns()) {
        counts += nextStartCount;
        if (column.isCharColumn()) {
          nextStartCount = nextStartCount + column.getNumUnique();
        } else {
          nextStartCount = nextStartCount + 1;
        }
      }
      val countsArr = counts.toArray
      _.map({
        case (x, y) => (x, y, {
          var index = 0;
          var row = new Array[Double](totalCols.value);
          for (column <- metadata.value.getQuasiColumns()) {
            if (column.isCharColumn()) {
              /*  println(row.size)
              println("=>"+(countsArr(index) + column.getRootCategory().getIndexOfColumnValue(y.get(column.getIndex()).get)))
              println("->"+y.get(column.getIndex()).get);*/
              row((countsArr(index) + column.getRootCategory().getIndexOfColumnValue(y.get(column.getIndex()).get))) = 1.0;
            } else {
              row(countsArr(index)) = ((y.get(column.getIndex()).get.toDouble) - column.getMin()) / (column.getRange());
            }
            index = index + 1;
          }
          unitVectors.value.map(unitVect => {
            (unitVect.zip(row).map({ case (x, y) => x * y }).sum) / r
          });
        })
      })
    });
  }
  /**
   * This method implements Strict K-Anonymity using LSH based bucketing technique.
   * The method uses LSH in order to hash the input dataset into buckets, then
   * It computes and assigns summary statistic for the bucket to all items.
   * It then reduces precision factor by  1/10'th of its original value and performs second
   * pass. It continues until precision factor size reaches 1.
   */
  def lsh(linesRDD: RDD[(Long, (String, Int, Int, String))], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String, numPartitons: Int) = {
    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);
    val zips = LBSMetadataWithSparkContext.getZip(sc);
    linesRDD.cache();
    var inputData = getBucketMappingNative(metadata, linesRDD);

    val buckets = getBucket(inputData);
    linesRDD.persist();
    buckets.persist(StorageLevel.MEMORY_AND_DISK_SER);
    val remaining = buckets.filter({ case (y) => y.size < numNeighbors });

    rdds.append(LSHUtil.assignSummaryStatisticToRDD(metadata, remaining.flatMap(x => x)))
    /**
     * We pickup buckets with size greater than or equal to "k" or number of neighbours
     */
    val neighbours = buckets.filter({ case (y) => y.size >= numNeighbors });
    /**
     * We assign summary statistic to all elements and then filter these elements.
     */
    rdds.append(neighbours.flatMap(x => LSHUtil.assignSummaryStatisticArray(metadata, x.toArray)));
    /**
     * The remaining values are re-grouped in next iteration.
     */
    /**
     * We assign the final summary statistic to all remaining entries.
     */
    val fileName = outputFilePath + "/LSH_" + numNeighborsVal.value + "_" + numHashFunctions + "_" + r + ".csv";
    val rdd = sc.union(rdds).sortByKey().map(_._2);

    new DataWriter(sc).writeRDDToAFile(fileName, rdd);
    val t1 = System.nanoTime()
    println("Time Taken: " + ((t1 - t0) / 1000000) + " :" + fileName);

    /*buckets.unpersist(true);
    */
    /**
     * Following code is for computation of the Information loss and thus is not included while calculating performance.
     */
    val linesRDDOP = new DataReader().readDataFile(sc, fileName, numPartitons);
    linesRDDOP.persist(StorageLevel.MEMORY_AND_DISK)
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    // println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));
    println((precisionFactor + "\t" + numHashFunctions + "\t" + (t1 - t0) / 1000000) + "\t" + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())) + "\t" + fileName);
  }
  /**
   * This method checks whether generalizedParent is valid parent of "neighbour"
   */
  def isNeighbourSubset(metadata: Metadata, generalizedParent: scala.collection.immutable.Map[Int, String], neighbour: scala.collection.immutable.Map[Int, String]): Boolean =
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
            if (column.isCharColumn()) {
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

  val ONE = 1.0;
  def extractRow(metadata: Metadata, values: Map[Int, String], totalCnt: Int, colStartCounts: Array[Int]): Array[Double] = {

    var index = 0;
    var row = new Array[Double](totalCnt);

    for (column <- metadata.getQuasiColumns()) {
      if (column.isCharColumn()) {
        row((colStartCounts(index) + column.getRootCategory().getIndexOfColumnValue(values.get(column.getIndex()).get))) = ONE;
      } else {
        row(colStartCounts(index)) = ((values.get(column.getIndex()).get.toDouble) - column.getMin()) / (column.getRange());
      }
      index = index + 1;
    }

    return row;
  }

}