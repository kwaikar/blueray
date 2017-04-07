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
 * This class implements three algorithms described in the thesis
 * LBS algorithm based on Game theoretical approach
 * LSH algorithm based on iterative LSH bucketing technique
 * LBS-LSH algorithm which uses LSH in order to expedite LBS algorithm.
 */
object LBSAndLSH {

  var sc: SparkContext = null;

  var numHashFunctions: Int = 10;
  var r: Double = 2.5;
  def main(args: Array[String]): Unit = {

    if (args.length < 10) {
      println("Program variables expected : <SPARK_MASTER> <HDFS_Data_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <numPartitions> <Algorithm(lbs/lbslsh/lsh)> <LSH_NUM_NEIGHBORS> <LSH_NUM_HASH_FUNCTIONS>")
    } else {
      sc = SparkSession
        .builder.appName("LBS").master(args(0)).getOrCreate().sparkContext;
      sc.setLogLevel("ERROR");
      numHashFunctions = args(9).toInt
      setup(args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7), args(8).toInt, args(6).toInt)

    }
  }

  def setup(hdfsFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: String, numNeighbours: Int, numParitions: Int) {
    var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, numParitions);
    linesRDD.persist(StorageLevel.MEMORY_AND_DISK)
    executeAlgorithm(outputFilePath, linesRDD, useLSH, lbsParam, numNeighbours, numParitions);
  }

  def executeAlgorithm(outputFilePath: String, linesRDD: RDD[(Long, Map[Int, String])], useLSH: String, lbsParam: LBSParameters, numNeighbours: Int, numPartitons: Int) {

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH.equalsIgnoreCase("lsh")) {
      lsh(linesRDD, lbsParam, numNeighbours, outputFilePath, numPartitons)

    } else if (useLSH.equalsIgnoreCase("lbslsh")) {
      lbslsh(linesRDD, lbsParam, outputFilePath);
    } else {
      /**
       * Execute LBS algorithm based on following paper.
       * Title = "A Game Theoretic Framework for Analyzing Re-Identification Risk"
       * Paper authors = {Zhiyu Wan,  Yevgeniy Vorobeychik,  Weiyi Xia,  Ellen Wright Clayton,  Murat Kantarcioglu,  Ranjit Ganta,  Raymond Heatherly,  Bradley A. Malin},
       * booktitle = {In ICDE},
       * year = {2015}
       */

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
          partition.map { case (x, y) => (x, algo.findOptimalStrategy(y)) }
        });
      output.persist(StorageLevel.MEMORY_AND_DISK);

      /**
       * We cache RDD as we do not want to be recomputed for each of the following three actions.
       */
      val publisherBenefit = output.map({ case (x, (y, z, t)) => (y) }).mean();
      linesRDD.unpersist(false);
      val advBenefit = output.map({ case (x, (y, z, t)) => (z) }).mean();
      val records = output.map({ case (x, (y, z, t)) => (x, t) });

      println("Avg PublisherPayOff found: " + publisherBenefit)
      println("Avg AdversaryBenefit found: " + advBenefit)
      val fileName = outputFilePath + "/LBS_" + lbsParam.V() + "_" + lbsParam.L() + "_" + lbsParam.C() + ".csv";

      new DataWriter(sc).writeRDDToAFile(fileName, records.sortByKey().values);

      val t1 = System.nanoTime()
      println("Time Taken: " + ((t1 - t0) / 1000000));
      output.unpersist(true);
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
  def getBuckets(metadata: Broadcast[Metadata], normalizedLinesRDD: RDD[(Long, Map[Int, String])], tuneDownFactor: Double): RDD[(Iterable[(Long, Map[Int, String])])] =
    {
      val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata.value));
      val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
      val unitVectors = getRandomUnitVectors(columnCounts);

      /**
       * For each row, compute the Hash Value.
       */
      val buckets = normalizedLinesRDD.map({
        case (x, y) => (
          {
            (x, y, LSHUtil.extractRow(metadata.value, y))
          })
      }).map({
        case (x, y, mappedY) => {
          val op = unitVectors.map(unitVect => {
            (Math.round(((unitVect.zip(mappedY).map({ case (x, y) => x * y }).sum) / r) * tuneDownFactor) / tuneDownFactor)
          });
          (op.mkString(","), /*Seq[(Long, scala.collection.immutable.Map[Int, String])]*/ /*(*/ (x, y) /*)*/ )
        }
      }).groupByKey().values; /*reduceByKey(_ ++ _)*/ /*.map(x => (x._2.toArray));*/ /* Group values by Concatenated Hash key.*/
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
    var inputData = linesRDD;
    var precisionFactor = 10000;
    /**
     * We hash entire dataset - this should lead to hashing of almost-duplicate entries into same bucket.
     */
    var buckets = getBuckets(metadata, inputData, precisionFactor);
    buckets.persist(StorageLevel.MEMORY_AND_DISK);
    val population = LBSMetadataWithSparkContext.getPopulation(sc);
    val zips = LBSMetadataWithSparkContext.getZip(sc);
    val hashedPopulation = LBSMetadataWithSparkContext.getHashedPopulation(sc);
      val params = sc.broadcast(lbsParam);
    val outputs = buckets.mapPartitions(partition =>
      {
        val algo = new LBSAlgorithm(metadata.value, params.value, population.value, zips.value, hashedPopulation.value);
        partition.flatMap(bucket =>
          {
            /**
             * We loop on each bucket in order to identify the ideal generalization level.
             * We take first entry, find out generalization level.
             */
            val itr = bucket.iterator;
            var list = ListBuffer[(Double, Double, Long, String)]();
            if (itr.hasNext) {
              var current = itr.next();
              var strategy = algo.findOriginalOptimalStrategy(current._2);

              var stringRepresentation = strategy._3.toArray.sortBy(_._1).map(_._2).mkString(",");
              list.append((strategy._1, strategy._2, current._1, stringRepresentation));
              /**
               * We loop on rest of the entries from the bucket and see if generalization level can be
               * applied or not. If not possible, it computes generalization for the same by invoking LBS algorithm.
               */

              while (itr.hasNext) {
                current = itr.next();
                if (isNeighbourSubset(metadata.value, strategy._3, current._2)) {

                  list.append((strategy._1, strategy._2, current._1, stringRepresentation));
                } else {
                  var childStrategy = algo.findOptimalStrategy(current._2);
                  list.append((childStrategy._1, childStrategy._2, current._1, childStrategy._3));
                }
              }

            }
            list
          });

      });

    outputs.persist(StorageLevel.MEMORY_AND_DISK);
    println("Checking pub benefit")
    buckets.unpersist(true)
    val publisherBenefit = outputs.map({ case (x, y, z, t) => (x) }).mean();

    println("Checking adv benefit")
    val advBenefit = outputs.map({ case (x, y, z, t) => (y) }).mean();
    val records = outputs.map({ case (x, y, z, t) => (z, t) }).sortByKey().map(_._2);

    println("Avg PublisherPayOff found: " + publisherBenefit)
    println("Avg AdversaryBenefit found: " + advBenefit)

    val fileName = outputFilePath + "LBSLSH_" + numHashFunctions + "_" + r + ".csv";
    new DataWriter(sc).writeRDDToAFile(fileName, records);

    val t1 = System.nanoTime()

    println("Time Taken: " + ((t1 - t0) / 1000000));
    outputs.unpersist(true)
  }

  /**
   * This method implements Strict K-Anonymity using LSH based bucketing technique.
   * The method uses LSH in order to hash the input dataset into buckets, then
   * It computes and assigns summary statistic for the bucket to all items.
   * It then reduces precision factor by  1/10'th of its original value and performs second
   * pass. It continues until precision factor size reaches 1.
   */
  def lsh(linesRDD: RDD[(Long, scala.collection.immutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String, numPartitons: Int) = {
    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);
    val zips = LBSMetadataWithSparkContext.getZip(sc);
    var inputData = linesRDD;
    /**
     * We start with 100000 as precision factor - which means bucket hash-value would be preserved upto 5 decimal places.
     * and hash all elements by calling getBuckets() function.
     */
    var precisionFactor = 100000.0;
    while (precisionFactor > 1) {
      var buckets = getBuckets(metadata, inputData, precisionFactor);
      buckets.persist(StorageLevel.MEMORY_AND_DISK);
      /**
       * We pickup buckets with size greater than or equal to "k" or number of neighbours
       */
      var neighbours = buckets.filter({ case (y) => y.size >= numNeighbors});
      /**
       * We assign summary statistic to all elements and then filter these elements.
       */
      var op = neighbours.flatMap(x => LSHUtil.assignSummaryStatistic(metadata, x.toArray));
      var remaining = buckets.filter({ case (y) => y.size < numNeighbors});
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
    var op = sc.parallelize(LSHUtil.assignSummaryStatistic(metadata, finalStep).toSeq);
    rdds.append(op);

    val fileName = outputFilePath + "/LSH_" + numNeighborsVal.value + "_" + numHashFunctions + "_" + r + ".csv";
    new DataWriter(sc).writeRDDToAFile(fileName, sc.union(rdds).sortByKey().map(_._2));
    val t1 = System.nanoTime()
    println("Time Taken: " + ((t1 - t0) / 1000000));
    /**
     * Following code is for computation of the Information loss and thus is not included while calculating performance.
     */
    val linesRDDOP = new DataReader().readDataFile(sc, fileName, numPartitons);
    linesRDDOP.persist(StorageLevel.MEMORY_AND_DISK)
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));
    linesRDDOP.unpersist(true);
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

}