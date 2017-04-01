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
      rdds = output._3
      totalPublisherPayOff = output._1
      totalAdvBenefit = output._2
    } else if (useLSH.equalsIgnoreCase("plain")) {
      val output = plainlsh(linesRDD, lbsParam, numNeighbours, outputFilePath);
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

  object ListAccumulator extends AccumulatorParam[List[Long]] {

    def zero(init: List[Long]): List[Long] = {
      return init
    }

    def addInPlace(l1: List[Long], l2: List[Long]): List[Long] = {
      l1 ::: l2
    }
  }

  val numHashFunctions: Int = 5;

  val w: Double =30.0;
  val b = 25.0; 

  def getBuckets(metadata: Broadcast[Metadata], normalizedLinesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]): RDD[(String, Iterable[(Long, scala.collection.mutable.Map[Int, String])])] =
    {
      val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata.value));

      val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
      val rand = new Random();
      val unitVectors: ListBuffer[Array[Double]] = ListBuffer();
      for (i <- 0 to numHashFunctions - 1) {
        {
          val value = Array.fill(columnCounts)(rand.nextGaussian());
          val total =1// Math.sqrt(value.reduce(Math.pow(_, 2) + Math.pow(_, 2)));
          unitVectors.append(value.map(x => x / total));
        }
      }
      println("NUMUnitVectors " + unitVectors.length);
      val buckets = normalizedLinesRDD.map({
        case (x, y) => {
          var bucket: StringBuilder = new StringBuilder();
          var mappedY = LSHUtil.extractRow(metadata.value, columnStartCounts.value, y, true);
          for (i <- 0 to numHashFunctions - 1) {
            var totalSum = 0.0;
            for (j <- 0 to columnCounts - 1) {
              totalSum = totalSum + unitVectors(i)(j) * mappedY(j);
            }
            var bucketId = 0;
            if (totalSum >= 0) {
              bucketId = 1;
            }
           // println(i + "|--|" + bucketId + " =" + totalSum + " " + numBuckets);
            bucket.append( Math.floor((totalSum+b)/w));
          }
          (bucket.toString().trim(),(x, y))
        }
      }).groupByKey();
      println(buckets.keys.collect().mkString("_"))
      buckets;
    }

  def lsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String): (Double, Double, List[RDD[(Int, String)]]) = {

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;
    var counter = 0;
    var predicted = 0;
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);
    /**
     * Build LSHmodel
     */
    val buckets = getBuckets(metadata, linesRDD).cache();
    println("Total count found: " + buckets.values.count());
    val op = buckets.values.flatMap(x => assignSummaryStatistic(x.toArray)).sortBy(_._1).map(_._2)

    new DataWriter(sc).writeRDDToAFile(outputFilePath, op);

    val linesRDDOP = new DataReader().readDataFile(sc, outputFilePath, true).cache();
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));

    /* val publisherBenefit = output.map({ case (x, y, z) => (x) }).mean();
    val advBenefit = output.map({ case (x, y, z) => (y) }).mean();
    val records = output.map({ case (x, y, z) => (z) });

    println("Avg PublisherPayOff found: " + publisherBenefit)
    println("Avg AdversaryBenefit found: " + advBenefit)
    new DataWriter(sc).writeRDDToAFile(outputFilePath, records);*/

    return (totalPublisherPayOff, totalAdvBenefit, rdds);
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
  def plainlsh(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], lbsParam: LBSParameters, numNeighbors: Int, outputFilePath: String) {

    val numNeighborsVal = sc.broadcast(numNeighbors);
    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;
    var counter = 0;
    var predicted = 0;

    var rdds: scala.collection.mutable.Map[Long, String] = scala.collection.mutable.Map();
    /**
     * Build LSHmodel
     */
    val metadata = LBSMetadata.getInstance();
    val columnStartCounts = sc.broadcast(LSHUtil.getColumnStartCounts(metadata));

    val inputToModel = linesRDD.map({
      case (x, y) => ({
        // println("YY=>"+y+"|");
        val row = LSHUtil.extractRow(metadata, columnStartCounts.value, y, true)
        (x.intValue(), Vectors.dense(row))
      })
    }).collect().toSeq

    val dataFrame = new SQLContext(sc).createDataFrame(inputToModel).toDF("id", "keys");
    val key = Vectors.dense(1.0, 0.0)
    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(30.0)
      .setNumHashTables(5)
      .setInputCol("keys")
      .setOutputCol("values")

    val model = brp.fit(dataFrame)
    val txModel = model.transform(dataFrame)

    val valuesGeneralized = sc.accumulator(List[Long]())(ListAccumulator);

    var outputMap: ListBuffer[RDD[(Long, (Double, Double, String))]] = ListBuffer();
    var originalPreds: ListBuffer[RDD[(Double, Double, Long, String)]] = ListBuffer();
    val rddsPartitioned = linesRDD.randomSplit((List.fill((linesRDD.count / 1000).toInt)(1.0)).toArray, 0);
    val metadatas = LBSMetadataWithSparkContext.getInstance(sc);
    val zips = LBSMetadataWithSparkContext.getZip(sc);
    val population = LBSMetadataWithSparkContext.getPopulation(sc);
    val count = linesRDD.count();

    var listOfNoNeighbours: scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Int, String]] = scala.collection.mutable.Map();
    var i: Int = 0;
    for (rddPartition <- rddsPartitioned) {
      var keysMapped = valuesGeneralized.value;
      var filteredRDD = rddPartition.filter({ case (x, y) => (!keysMapped.contains(x)) }).collect();
      for (singleOutput <- filteredRDD) {

        if (!keysMapped.contains(singleOutput._1) && count > valuesGeneralized.value.size) {
          var neighborsRow = model.approxNearestNeighbors(txModel, Vectors.dense(LSHUtil.extractRow(LBSMetadata.getInstance(), columnStartCounts.value, singleOutput._2, true)), 2 * numNeighborsVal.value).collectAsList().asInstanceOf[java.util.List[Row]]
          var nebMap = sc.parallelize(neighborsRow.toArray.asInstanceOf[Array[Row]]).map({ case (row) => (row(0).asInstanceOf[Int].longValue(), LSHUtil.extractReturnObject(metadata, columnStartCounts.value, (row(1).asInstanceOf[DenseVector]).values)) }).filter({ case (x, y) => (!keysMapped.contains(x)) }).take(numNeighborsVal.value);
          if (nebMap.size >= numNeighborsVal.value) {

            rdds.++=(assignSummaryStatistic(nebMap));

          } else {

            listOfNoNeighbours.++=(nebMap);
            if (listOfNoNeighbours.size >= numNeighborsVal.value) {
              for (key <- listOfNoNeighbours.keys.toArray) {
                valuesGeneralized += List(key);
              }
              rdds.++=(assignSummaryStatistic(listOfNoNeighbours.toArray));
              listOfNoNeighbours = scala.collection.mutable.Map();
            }
          }
          keysMapped = valuesGeneralized.value;
        }
        //  println("Current Count of Generalized records:" + valuesGeneralized.value.size );
      }
      //  println("Current Count of Generalized records:" + valuesGeneralized.value.size + "  Original RDD:"+filteredRDD.size);
    }
    rdds.++=(listOfNoNeighbours.map(x => (x._1, "*,37010.0_72338.0,0_120,*")));
    val output = sc.parallelize(rdds.toSeq.sortBy(_._1)).map(_._2)

    new DataWriter(sc).writeRDDToAFile(outputFilePath, output);

    val linesRDDOP = new DataReader().readDataFile(sc, outputFilePath, true).cache();
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));

    return (totalPublisherPayOff, totalAdvBenefit, output);
  }

  def assignSummaryStatistic(nebMapArr: Array[(Long, scala.collection.mutable.Map[Int, String])]): Map[Long, String] =
    {
      println("Array size found " + nebMapArr.size);
      val metadata = LBSMetadata.getInstance();

      val nebMap = sc.parallelize(nebMapArr);
      var indexValueGroupedIntermediate = nebMap.flatMap({ case (x, y) => y }).groupByKey().map({ case (index, list) => (index, list.toList.distinct) })

      val indexValueGrouped = indexValueGroupedIntermediate.map({
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

      var map: scala.collection.Map[Int, String] = indexValueGrouped.collectAsMap();
      return nebMap.map({
        case (x, y) =>

          /**
           * Append column values to sb.
           */

          for (i <- 0 to metadata.numColumns() - 1) {

            if (metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
              /* 
               * Summary statistic for the quasi-identifier without any cut on current column.
               */

              if (y.get((metadata.numColumns() + i)) == None) {
                y.remove(i);
                y.put(i, map.get(i).get);
              } else {
                y.remove(i);
                y.put(i, y.get(metadata.numColumns() + i).get);
                y.remove(metadata.numColumns() + i);
              }
            }
          }
          (x, y.toArray.sortBy(_._1).map(_._2).mkString(","))
      }).collect().toMap;
    }

}