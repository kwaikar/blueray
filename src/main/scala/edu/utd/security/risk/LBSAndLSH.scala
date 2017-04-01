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
      println("Program variables expected : <SPARK_MASTER> <HDFS_Data_file_path> <output_file_path> <recordCost> <maxPublisherBenefit> <publishersLoss> <adversaryAttackCost> <USE_LSH(true/false)> <LSH_NUM_NEIGHBORS>")
    } else {
      val t0 = System.nanoTime()

      sc = SparkSession
        .builder.appName("LBS").master(args(0)).getOrCreate().sparkContext;
      sc.setLogLevel("ERROR");
      setup(args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7), args(8).toInt)

      val t1 = System.nanoTime()

      println("Time Taken: " + ((t1 - t0) / 1000000));
    }
  }

  def setup(hdfsFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: String, numNeighbours: Int) {
    var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, true).cache();
    lbs(outputFilePath, linesRDD, useLSH, lbsParam, numNeighbours);
  }
  def lbs(outputFilePath: String, linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], useLSH: String, lbsParam: LBSParameters, numNeighbours: Int) {

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH.equalsIgnoreCase("true")) {
      val output = lsh(linesRDD, lbsParam, numNeighbours, outputFilePath)
    } else if (useLSH.equalsIgnoreCase("plain")) {
      //  val output = plainlsh(linesRDD, lbsParam, numNeighbours, outputFilePath);
    } else {

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
      val output = linesRDD.map({ case (x, y) => (x, new LBSAlgorithmWithSparkContext(zips, population, metadata, lbsParam).findOptimalStrategy(y)) }).sortByKey().values.cache();
      val publisherBenefit = output.map({ case (x, y, z) => (x) }).mean();
      val advBenefit = output.map({ case (x, y, z) => (y) }).mean();
      val records = output.map({ case (x, y, z) => (z) });

      println("Avg PublisherPayOff found: " + publisherBenefit)
      println("Avg AdversaryBenefit found: " + advBenefit)
      new DataWriter(sc).writeRDDToAFile(outputFilePath, records);

    }

  }

  val numHashFunctions: Int = 10;

  val r: Double = 30.0;
  val b: ListBuffer[Double] = ListBuffer();

  def getBuckets(metadata: Broadcast[Metadata], normalizedLinesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], tuneDownFactor: Double): RDD[(String, Array[(Long, scala.collection.mutable.Map[Int, String])])] =
    {
      val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata.value));

      val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
      val rand = new Random();
      /*  val unitVectors: ListBuffer[Array[Double]] = ListBuffer();
      for (i <- 0 to numHashFunctions - 1) {
        {
          val value = Array.fill(columnCounts)(rand.nextGaussian());
          val total = Math.sqrt(value.map(x => (x * x)).sum);
          val op = value.map(x => x / total)
          unitVectors.append(op);
          b.append(Math.random() * r);
        }
      }*/

      val randUnitVectors: Array[Array[Double]] = {
        Array.fill(numHashFunctions) {
          val randArray = Array.fill(columnCounts)(rand.nextGaussian())
          normalize(breeze.linalg.Vector(randArray)).toArray
        }
      }

      /*println("NUMUnitVectors " + unitVectors.length);*/
      val buckets = normalizedLinesRDD.map({
        case (x, y) => {
          var bucket: ListBuffer[Double] = new ListBuffer();
          var mappedY = LSHUtil.extractRow(metadata.value, columnStartCounts.value, y, true);
          val hashValues: Array[Double] = randUnitVectors.map(unitVec => {
            var totalSum = 0.0;
            for (j <- 0 to columnCounts - 1) {
              totalSum = totalSum + unitVec(j) * mappedY(j);
            }
            val op = Math.round((totalSum / r) * tuneDownFactor) / tuneDownFactor
            op
          })
          // TODO: Output vectors of dimension numHashFunctions in SPARK-18450

          (hashValues.mkString(","), (x, y))
        }
      }).groupByKey().map(x => (x._1, x._2.toArray));
      buckets;
    }

  def lsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String)= {

    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);
    //  sc.setLogLevel("DEBUG");

    var inputData = linesRDD;
    var numBuckets = 100000.0;
    while (numBuckets > 1) {
      println("NumBuckets: "+numBuckets +" : "+inputData.count());
      var buckets = getBuckets(metadata, inputData,numBuckets);
      buckets.cache();
      var neighbours = buckets.filter({ case (x, y) => y.size >= numNeighborsVal.value });
      var op = neighbours.map(x => (x._2)).flatMap(x => assignSummaryStatistic(x));
      var remaining = buckets.filter({ case (x, y) => y.size < numNeighborsVal.value });
      inputData = remaining.flatMap(_._2)
      rdds.append(op);
      buckets.unpersist();

      numBuckets = numBuckets / 10.0;
    }
    val finalStep =inputData.collect();
    println("Final step size"+finalStep.size)
    var op = sc.parallelize(assignSummaryStatistic(finalStep).toSeq);
    rdds.append(op);

    new DataWriter(sc).writeRDDToAFile(outputFilePath, sc.union(rdds).sortByKey().map(_._2));

    val linesRDDOP = new DataReader().readDataFile(sc, outputFilePath, true).cache();
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));

    /* val publisherBenefit = output.map({ case (x, y, z) => (x) }).mean();
    val advBenefit = output.map({ case (x, y, z) => (y) }).mean();
    val records = output.map({ case (x, y, z) => (z) });

    println("Avg PublisherPayOff found: " + publisherBenefit)
    println("Avg AdversaryBenefit found: " + advBenefit)
    new DataWriter(sc).writeRDDToAFile(outputFilePath, records);*/

  }

  def isNeighbourSubset(metadata: Metadata, generalizedParent: scala.collection.mutable.Map[Int, String], neighbour: scala.collection.mutable.Map[Int, String]): Boolean =
    {
      var neighborIsSubSet = true;

      breakable {
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

  def assignSummaryStatistic(nebMapArr: Array[(Long, scala.collection.mutable.Map[Int, String])]): Map[Long, String] =
    {
      val metadata = LBSMetadata.getInstance();

      var indexValueGroupedIntermediate = nebMapArr.flatMap({ case (x, y) => y }).groupBy(_._1).map(x => (x._1, x._2.map(_._2)));
      var int2 = indexValueGroupedIntermediate.map({ case (index, list) => (index, list.toList.distinct) })

      var indexValueGrouped = indexValueGroupedIntermediate.map({
        case (x, y) =>
          val column = metadata.getMetadata(x).get;
          if (column.getIsQuasiIdentifier()) {
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
          } else { /*99752 19340*/ /*818 094 4710*/
            (-1, "")
          }
      });
      var map: scala.collection.Map[Int, String] = indexValueGrouped;
      return nebMapArr.clone().map({
        case (x, y) =>

          for (i <- 0 to metadata.numColumns() - 1) {

            if (metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
              /* 
               * Summary statistic for the quasi-identifier without any cut on current column.
               */
              y.remove(i);
              y.put(i, map.get(i).get);
            }
          }
          (x, y.toArray.sortBy(_._1).map(_._2).mkString(","))
      }).toMap;
    }

}