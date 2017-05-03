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
import java.util.concurrent.ConcurrentHashMap

/**
 * This class implements three algorithms described in the thesis.
 * LBS algorithm - based on Game theoretical approach
 * LSH algorithm - based on iterative LSH bucketing technique
 * LBS-LSH algorithm - uses LSH in order to expedite LBS algorithm.
 */
object LBSAndLSH {

  var sc: SparkContext = null;

  var numHashFunctions: Int = 6;
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
      numHashFunctions = args(10).toInt
      precisionFactor = args(9).toInt;
      executeAlgorithm(args(1), args(2), new LBSParameters(args(3).toDouble, args(4).toDouble, args(5).toDouble), args(7), args(8).toInt, args(6).toInt)
      /*}
      }

*/ }
  }

  def executeAlgorithm(hdfsFilePath: String, outputFilePath: String, lbsParam: LBSParameters, useLSH: String, numNeighbours: Int, numPartitions: Int) {

    var totalPublisherPayOff = 0.0;
    var totalAdvBenefit = 0.0;

    var list: ListBuffer[(Int, String)] = ListBuffer();
    var rdds: List[RDD[(Int, String)]] = List();
    if (useLSH.equalsIgnoreCase("lsh")) {
      lsh(hdfsFilePath, numPartitions, numNeighbours, outputFilePath)
    } else if (useLSH.equalsIgnoreCase("lbslsh")) {
      var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, numPartitions);

      lbslsh(hdfsFilePath, lbsParam, numPartitions, numNeighbours, outputFilePath)
    } else if (useLSH.equalsIgnoreCase("lshfrompaper")) {
      var linesRDD = new DataReader().readDataFile(sc, hdfsFilePath, numPartitions);
      lshfrompaper(hdfsFilePath, numPartitions, numNeighbours, outputFilePath)

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

      val file = sc.textFile(hdfsFilePath, numPartitions)
      val lines = file.flatMap(_.split("\n")).zipWithIndex()
      /**
       * Here, we calculate the optimal Generalization level for entire RDD.
       */
      val output = lines.mapPartitions(partition =>
        {
          val algo = new LBSAlgorithm(metadata.value, params.value, population.value, zips.value, hashedPopulation.value);
          partition.map {
            case (x, y) => {
              val strategy = algo.findOptStrategy(x);
              (y, strategy._1, strategy._2, strategy._3)
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

  /**
   * This method is used for using LSH bucketing feature for improving performance of LBS algorithm.
   */
  def lbslsh(hdfsFilePath: String, lbsParam: LBSParameters, numPartitions: Int, numNeighbors: Int, outputFilePath: String) = {

    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();

    val metadata = LBSMetadataWithSparkContext.getInstance(sc);

    /**
     * The algorithm starts by specifying precision factor as a high value in order to get minimum number of
     * false positives.
     */
    var precisionFactor = 10000;

    var buckets = getLBSLSHData(sc, lbsParam, metadata, hdfsFilePath, numPartitions, precisionFactor, numNeighbors);

    /**
     * We hash entire dataset - this should lead to hashing of almost-duplicate entries into same bucket.
     */

    val fileName = outputFilePath + "/LBSLSH_" + numHashFunctions + "_" + r + ".csv";
    new DataWriter(sc).writeRDDToAFile(fileName, buckets);
    val t1 = System.nanoTime()
    println("Output written to file :" + fileName);
    println("Time Taken: " + ((t1 - t0) / 1000000));

  }
  def partition(lines: RDD[(String, Long)], metadata: Broadcast[Metadata], hashes: Broadcast[Array[(Int, Int)]], countsArr: Broadcast[Array[Int]], totalCols: Broadcast[Int]): RDD[(String, Array[(ListBuffer[Int],String, Long)])] =
    {
      lines.mapPartitions({

        _.map({
          case (x, y) => {
            var index = 0;
            //  var row = new Array[Double](totalCols.value);
            val split = x.split(",")
            val arr = Array[Int](0, 1, 2, 3);
            val listBuffer = new ListBuffer[Int]();
            for (index <- arr) {
              var column = metadata.value.getMetadata(index).get;
              var key = split(index)
              if (!column.isCharColumn()) {
                key = key.toDouble.toString()
              }
              val indices = column.getRootCategory().getParentIndexOfColumnValue(key);
              // println("index" + index + " :" + indices.mkString(","))
              for (arrIndex <- indices) {
                listBuffer.+=((countsArr.value(index) + arrIndex));
                // row((countsArr.value(index) + arrIndex)) = 1.0;
              }
            }

            val concatenatedBucket = hashes.value.map(hash => {
              listBuffer.map(index => (hash._1 * index / hash._2) % totalCols.value).min;
              //Math.round(((unitVect.zip(row).map({ case (x, y) => x * y }).sum) / r) * precisionFactor) / precisionFactor
            }).mkString(",");
            println(concatenatedBucket + ":" + x);
            (concatenatedBucket, (Array[(ListBuffer[Int],String, Long)]((listBuffer,x, y))))
            //      (concatenatedBucket, (Array[Long](y), (Set[String](split(0)), Array.fill(2)(split(1).toInt), Array.fill(2)(split(2).toInt), Set[String](split(3)))))
          }
        })
      }).reduceByKey({
        case (x, y) => {
          (x.union(y))
        }
      });
    }

  def partitionArray(lines: Array[(ListBuffer[Int],String, Long)], metadata: Broadcast[Metadata], hashes: Broadcast[Array[(Int, Int)]], countsArr: Broadcast[Array[Int]], totalCols: Broadcast[Int]): scala.collection.immutable.Map[String, Array[(ListBuffer[Int],String, Long)]] =
    {

      lines.map({
        case (p,x, y) => {
          var index = 0;
          //  var row = new Array[Double](totalCols.value);
          val split = x.split(",")
          val arr = Array[Int](0, 1, 2, 3);
          val listBuffer = new ListBuffer[Int]();
          for (index <- arr) {
            var column = metadata.value.getMetadata(index).get;
            var key = split(index)
            if (!column.isCharColumn()) {
              key = key.toDouble.toString()
            }
            val indices = column.getRootCategory().getParentIndexOfColumnValue(key);
            // println("index" + index + " :" + indices.mkString(","))
            for (arrIndex <- indices) {
              listBuffer.+=((countsArr.value(index) + arrIndex));
              // row((countsArr.value(index) + arrIndex)) = 1.0;
            }
          }

          val concatenatedBucket = hashes.value.map(hash => {
            listBuffer.map(index => (hash._1 * index / hash._2) % totalCols.value).min;
            //Math.round(((unitVect.zip(row).map({ case (x, y) => x * y }).sum) / r) * precisionFactor) / precisionFactor
          }).mkString(",");
          println(concatenatedBucket + ":" + x);
          (concatenatedBucket, (listBuffer,x, y))
          //      (concatenatedBucket, (Array[Long](y), (Set[String](split(0)), Array.fill(2)(split(1).toInt), Array.fill(2)(split(2).toInt), Set[String](split(3)))))
        }
      }).groupBy(_._1).mapValues({ case (Array(x, (y, z))) => (Array(z)) }) /*({
        case (x, y) => {
          (x.union(y))
        }
      });*/
    }

  def lsh_RC(lines: RDD[(String, Long)], metadata: Broadcast[Metadata], hashes: Broadcast[Array[(Int, Int)]], countsArr: Broadcast[Array[Int]], totalCols: Broadcast[Int], numNeighbors: Broadcast[Int]): RDD[(Long, String)] =
    {
      val generalizedBucket = partition(lines, metadata, hashes, countsArr, totalCols);
      val output = generalizedBucket.map({
        case (x, y) => {
          var remaining: ListBuffer[(String,Array[(ListBuffer[Int],String, Long)])] = ListBuffer();
          var summarize: ListBuffer[Array[(String, Long)]] = ListBuffer();

          if (y.size < numNeighbors.value) {
            remaining.+=((x,y));
          } else if (y.size == numNeighbors.value) {
            summarize.+=(y.map(x=>(x._2,x._3)));
          } else {
            val output = partitionArray(y , metadata, hashes, countsArr, totalCols);

            val op = output.map({
              case (p, q) =>
                {
                  var remainingQ: ListBuffer[(String,Array[(ListBuffer[Int],String, Long)])] = ListBuffer();
                  var summarizeQ: ListBuffer[Array[(String, Long)]] = ListBuffer();

                  if (q.size < numNeighbors.value) {
                    remainingQ.+=((p,q));
                  } else if (q.size == numNeighbors.value) {
                    summarizeQ.+=(q.map(x=>(x._2,x._3)));
                  } else {
                    val recursionOP = lsh_RC(q , metadata, hashes, countsArr, totalCols, numNeighbors);
                    for (entry <- recursionOP) {
                      remainingQ.++=(entry._1);
                      for (value <- entry._2) {
                        summarizeQ.+=(value);
                      }
                    }

                  }
                  (remainingQ, summarizeQ)
                }
            });
            val opp = op.keys.flatten.toArray
            remaining.union(opp);
            summarize.++=(op.values.flatten);

          }
          (remaining, summarize)
        }
      });
      /**
       * Perform agglomerative clustering on small blocks
       */
      val mergedEntries = sc.parallelize(output.map(_._1).reduce(_ ++ _)).values.zipWithIndex();
      val cartesian = mergedEntries.cartesian(mergedEntries);
      val distance = cartesian.map({
        case (((entries1), id1), ((entries2), id2)) => {
          var maxDist =Integer.MAX_VALUE;
          
        for(entry1<-entries1)
        {
          for(entry2<-entries2)
          {
            val size = entry1._1.intersect(entry2._1).size
            if(size<maxDist)
            {
              maxDist =size;
            }
          }
        }
        (id1,id2,((entries1.size+entries2.size -numNeighbors.value)*(1/numNeighbors.value)+1)*(maxDist+2*entries1(1)._1.size),entries1.union(entries2))
        }
      });

      val merged = output.map(_._2).reduce(_ ++ _);
      // apply aglomerative clustering in merged
      val op = merged.map(LSHUtil.assignSummaryStatisticToPlainData(metadata, _)).reduce(_ ++ _);
      sc.parallelize(op)
    }

  def lsh_RC(lines: Array[(ListBuffer[Int],String, Long)], metadata: Broadcast[Metadata], hashes: Broadcast[Array[(Int, Int)]], countsArr: Broadcast[Array[Int]], totalCols: Broadcast[Int], numNeighbors: Broadcast[Int]): Map[ListBuffer[(String,Array[(ListBuffer[Int],String, Long)])], ListBuffer[Array[(String, Long)]]] =
    {
      val generalizedBucket = partitionArray(lines, metadata, hashes, countsArr, totalCols);
      val output = generalizedBucket.map({
        case (x, y) => {
          var remaining: ListBuffer[(String,Array[(ListBuffer[Int],String, Long)])] = ListBuffer();
          var summarize: ListBuffer[Array[(String, Long)]] = ListBuffer();
          var divide: Array[(String, Long)] = Array();

          if (y.size < numNeighbors.value) {
            remaining.+=((x,y));
          } else if (y.size == numNeighbors.value) {
            summarize.+=(y.map(x=>(x._2,x._3)));
          } else {
            val output = partitionArray(y , metadata, hashes, countsArr, totalCols);

            val op = output.map({
              case (p, q) =>
                {
                  var remainingQ: ListBuffer[(String,Array[(ListBuffer[Int],String, Long)])] = ListBuffer();
                  var summarizeQ: ListBuffer[Array[(String, Long)]] = ListBuffer();
                  var divideQ: Array[(String, Long)] = Array();

                  if (q.size < numNeighbors.value) {
                    remainingQ.+=((p,q));
                  } else if (q.size == numNeighbors.value) {
                    summarizeQ.+=(q.map(x=>(x._2,x._3)));
                  } else {
                    val recursionOP = lsh_RC(q, metadata, hashes, countsArr, totalCols, numNeighbors);
                    for (entry <- recursionOP) {
                      remainingQ.++=(entry._1); 
                      for (value <- entry._2) {
                        summarizeQ.+=(value);
                      }
                    }
                  }
                  (remainingQ, summarizeQ)
                }
            });
            val remainingEntries = op.keys.flatten.toArray
            remaining.union(remainingEntries);
            val summaryValues = op.values.flatten.toArray
            summarize.++=(summaryValues);
          }
          (remaining, summarize)
        }
      });
      output
    }
  def getLSHPaperData(metadata: Broadcast[Metadata], hdfsFilePath: String, numPartitions: Int, precisionFactor: Double, numNeighbors: Broadcast[Int]): RDD[(Long, String)] = {

    val file = sc.textFile(hdfsFilePath, numPartitions)
    val lines = file.flatMap(_.split("\n")).zipWithIndex()
    val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);

    val rand = new Random();

    val hashfunctions = Array.fill(numHashFunctions)(rand.nextInt(10000), rand.nextInt(10000));

    val hashes = sc.broadcast(hashfunctions);
    var nextStartCount = 0;
    val counts = ListBuffer[Int]();
    var totalCount = 0;
    for (column <- metadata.value.getQuasiColumns()) {
      counts += nextStartCount;
      column.getRootCategory().populateMapIfRequired()
      nextStartCount = nextStartCount + column.getRootCategory().parentMap.size();
      totalCount += column.getRootCategory().parentMap.size();
    }
    var totalCols = sc.broadcast(totalCount);
    var countsArr = sc.broadcast(counts.toArray);
    println("countsArr" + countsArr.value.mkString(","));

    val op = lsh_RC(lines, metadata, hashes, countsArr, totalCols, numNeighbors);

    /* val op = generalizedBucket.values.flatMap({
      case ((ids, (genders, zips, ages, races))) =>
        {
          if (ids.size >= numNeighbors.value) {

            var column = metadata.value.getMetadata(0).get;

            var generalization = column.findCategory(genders.toArray).value()
            var min = zips.min;
            var max = zips.max;
            if (min == max) {
              generalization += "," + min;
            } else {
              generalization += "," + min + "_" + max;
            }
            min = ages.min;
            max = ages.max;
            if (min == max) {
              generalization += "," + min;
            } else {
              generalization += "," + min + "_" + max;
            }
            column = metadata.value.getMetadata(3).get;
            generalization += "," + column.findCategory(races.toArray).value()
            ids.map(x => (x, generalization));
          } else {

            ids.map(x => (x, "*,37010_72338,0_120,*"));
          }

        }
    });*/
    return op;
  }

  def getLSHedData(metadata: Broadcast[Metadata], hdfsFilePath: String, numPartitions: Int, precisionFactor: Double, numNeighbors: Broadcast[Int]): RDD[(Long, String)] = {

    val file = sc.textFile(hdfsFilePath, numPartitions)
    val lines = file.flatMap(_.split("\n")).zipWithIndex()
    val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
    val unitVectors = sc.broadcast(getRandomUnitVectors(columnCounts));

    var totalCols = sc.broadcast(LSHUtil.getTotalNewColumns(metadata.value));

    val generalizedBucket = lines.mapPartitions({
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
      val countsArr = counts.toArray;

      _.map({
        case (x, y) => {
          var index = 0;
          var row = new Array[Double](totalCols.value);
          val split = x.split(",")
          var column = metadata.value.getMetadata(0).get;
          row((countsArr(0) + column.getRootCategory().getIndexOfColumnValue(split(0)))) = 1.0;
          column = metadata.value.getMetadata(3).get;
          row((countsArr(3) + column.getRootCategory().getIndexOfColumnValue(split(3)))) = 1.0;
          column = metadata.value.getMetadata(1).get;
          row(countsArr(1)) = ((split(1).toDouble) - column.getMin()) / (column.getRange());
          column = metadata.value.getMetadata(2).get;
          row(countsArr(2)) = ((split(2).toDouble) - column.getMin()) / (column.getRange());

          val concatenatedBucket = unitVectors.value.map(unitVect => {
            Math.round(((unitVect.zip(row).map({ case (x, y) => x * y }).sum) / r) * precisionFactor) / precisionFactor
          }).mkString(",");
          println(concatenatedBucket + ":" + x);
          (concatenatedBucket, (Array[Long](y), (Set[String](split(0)), Array.fill(2)(split(1).toInt), Array.fill(2)(split(2).toInt), Set[String](split(3)))))
        }
      })
    }).reduceByKey({
      case ((id1, (a, b, c, d)), (id2, (p, q, r, s))) => {
        var bq = Array[Int](b(0), b(1));
        if (b(0) > q(0)) {
          bq(0) = q(0);
        }
        if (b(1) < q(1)) {
          bq(1) = q(1);
        }

        var cr = Array[Int](c(0), c(1));
        if (c(0) > r(0)) {
          cr(0) = r(0);
        }
        if (c(1) < r(1)) {
          cr(1) = r(1);
        }
        (id1.union(id2), (a.union(p), bq, cr, d.union(s)))
      }
    });

    val op = generalizedBucket.values.flatMap({
      case ((ids, (genders, zips, ages, races))) =>
        {
          if (ids.size >= numNeighbors.value) {

            var column = metadata.value.getMetadata(0).get;

            var generalization = column.findCategory(genders.toArray).value()
            var min = zips.min;
            var max = zips.max;
            if (min == max) {
              generalization += "," + min;
            } else {
              generalization += "," + min + "_" + max;
            }
            min = ages.min;
            max = ages.max;
            if (min == max) {
              generalization += "," + min;
            } else {
              generalization += "," + min + "_" + max;
            }
            column = metadata.value.getMetadata(3).get;
            generalization += "," + column.findCategory(races.toArray).value()
            ids.map(x => (x, generalization));
          } else {

            ids.map(x => (x, "*,37010_72338,0_120,*"));
          }

        }
    });
    return op;
  }

  def getLBSLSHData(sc: SparkContext, lbsParam: LBSParameters, metadata: Broadcast[Metadata], hdfsFilePath: String, numPartitions: Int, precisionFactor: Double, numNeighbors: Int): RDD[(String)] = {

    val population = LBSMetadataWithSparkContext.getPopulation(sc);
    val zips = LBSMetadataWithSparkContext.getZip(sc);
    val hashedPopulation = LBSMetadataWithSparkContext.getHashedPopulation(sc);
    val params = sc.broadcast(lbsParam);

    val file = sc.textFile(hdfsFilePath, numPartitions)
    val lines = file.flatMap(_.split("\n")).zipWithIndex()
    val columnCounts = LSHUtil.getTotalNewColumns(metadata.value);
    val unitVectors = sc.broadcast(getRandomUnitVectors(columnCounts));

    var totalCols = sc.broadcast(LSHUtil.getTotalNewColumns(metadata.value));

    val generalizedBucket = lines.mapPartitions({
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
      val countsArr = counts.toArray;

      _.map({
        case (x, y) => {
          var index = 0;
          var row = new Array[Double](totalCols.value);
          val split = x.split(",")
          var column = metadata.value.getMetadata(0).get;
          row((countsArr(0) + column.getRootCategory().getIndexOfColumnValue(split(0)))) = 1.0;
          column = metadata.value.getMetadata(3).get;
          row((countsArr(3) + column.getRootCategory().getIndexOfColumnValue(split(3)))) = 1.0;
          column = metadata.value.getMetadata(1).get;
          row(countsArr(1)) = ((split(1).toDouble) - column.getMin()) / (column.getRange());
          column = metadata.value.getMetadata(2).get;
          row(countsArr(2)) = ((split(2).toDouble) - column.getMin()) / (column.getRange());

          val concatenatedBucket = unitVectors.value.map(unitVect => {
            Math.round(((unitVect.zip(row).map({ case (x, y) => x * y }).sum) / r) * precisionFactor) / precisionFactor
          }).mkString(",");
          (concatenatedBucket, Array[(Long, String, Int, Int, String)]((y, split(0), split(1).toInt, split(2).toInt, split(3))))
        }
      })
    }).reduceByKey({
      case ((a), (b)) => {
        a.union(b)
      }
    });

    val op = generalizedBucket.values.mapPartitions({
      partition =>
        {

          val algo = new LBSAlgorithm(metadata.value, params.value, population.value, zips.value, hashedPopulation.value);
          partition.flatMap(array =>
            {
              val itr = array.iterator;
              var list = ListBuffer[(Long, (Double, Double, String))]();
              if (itr.hasNext) {
                var current = itr.next();
                var map = Map(0 -> current._2, 1 -> current._3.toString(), 2 -> current._4.toString(), 3 -> current._5);
                var strategy = algo.findOriginalOptimalStrategy(map);
                var stringRepresentation = strategy._3.toArray.sortBy(_._1).map(_._2).mkString(",");
                list.append((current._1, (strategy._1, strategy._2, stringRepresentation)));
                /**
                 * We loop on rest of the entries from the bucket and see if generalization level can be
                 * applied or not. If not possible, it computes generalization for the same by invoking LBS algorithm.
                 */
                while (itr.hasNext) {
                  current = itr.next();
                  map = Map(0 -> current._2, 1 -> current._3.toString(), 2 -> current._4.toString(), 3 -> current._5);

                  if (isNeighbourSubset(metadata.value, strategy._3, map)) {
                    list.append((current._1, (strategy._1, strategy._2, stringRepresentation)));
                  } else {
                    var childStrategy = algo.findOptimalStrategy(map);
                    list.append((current._1, (childStrategy._1, childStrategy._2, childStrategy._3)));
                  }
                }
              }
              list;
            })
        }
    });
    op.cache();
    println("Publisher benefit found: " + op.map(_._2._1).mean());
    println("Adversary benefit found: " + op.map(_._2._2).mean());
    return op.sortByKey().map(x => (x._2._3));
  }

  def lshfrompaper(hdfsFilePath: String, numPartitions: Int, numNeighbors: Int, outputFilePath: String): Boolean = {
    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);
    val fileName = outputFilePath + "/LSH_" + numNeighborsVal.value + "_" + numHashFunctions + "_" + r + ".csv";

    var buckets = getLSHPaperData(metadata, hdfsFilePath, numPartitions, precisionFactor, numNeighborsVal);

    val rdd = buckets.sortByKey().map(_._2);
    new DataWriter(sc).writeRDDToAFile(fileName, rdd);
    val t1 = System.nanoTime()
    println("Time Taken: " + ((t1 - t0) / 1000000) + " :" + fileName);

    /**
     * Following code is for computation of the Information loss and thus is not included while calculating performance.
     */
    val linesRDDOP = new DataReader().readDataFile(sc, fileName, numPartitions);
    linesRDDOP.persist(StorageLevel.MEMORY_AND_DISK)
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    // println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));
    println((precisionFactor + "\t" + numHashFunctions + "\t" + (t1 - t0) / 1000000) + "\t" + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())) + "\t" + fileName);
    return true;
  }

  def lsh(hdfsFilePath: String, numPartitions: Int, numNeighbors: Int, outputFilePath: String): Boolean = {
    val t0 = System.nanoTime()
    var rdds: ListBuffer[RDD[(Long, String)]] = ListBuffer();
    val numNeighborsVal = sc.broadcast(numNeighbors);
    val metadata = LBSMetadataWithSparkContext.getInstance(sc);
    val fileName = outputFilePath + "/LSH_" + numNeighborsVal.value + "_" + numHashFunctions + "_" + r + ".csv";

    var buckets = getLSHedData(metadata, hdfsFilePath, numPartitions, precisionFactor, numNeighborsVal);

    val rdd = buckets.sortByKey().map(_._2);
    new DataWriter(sc).writeRDDToAFile(fileName, rdd);
    val t1 = System.nanoTime()
    println("Time Taken: " + ((t1 - t0) / 1000000) + " :" + fileName);

    /**
     * Following code is for computation of the Information loss and thus is not included while calculating performance.
     */
    val linesRDDOP = new DataReader().readDataFile(sc, fileName, numPartitions);
    linesRDDOP.persist(StorageLevel.MEMORY_AND_DISK)
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    // println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));
    println((precisionFactor + "\t" + numHashFunctions + "\t" + (t1 - t0) / 1000000) + "\t" + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())) + "\t" + fileName);
    return true;
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

}