package edu.utd.security.risk

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import edu.utd.security.mondrian.DataWriter
import edu.utd.security.risk.Dimensions

/**
 * This is implementation of paper on Mondrian multi-dimensional paritioning for K-Anonymity
 * Paper authors = {Kristen Lefevre and David J. Dewitt and Raghu Ramakrishnan},
 * Paper title = {Mondrian multidimensional k-anonymity},
 * booktitle = {In ICDE},
 * year = {2006}
 */
object Mondrian {

  /**
   * This method accepts 4 parameters -
   * 1. hdfs File path
   * 2. Metadata file
   * 3. Output File path
   * 4. value of k
   */

  
  def main(args: Array[String]): Unit = {

    val t0 = System.nanoTime()

    sc = SparkSession
      .builder.appName("Mondrian").master(args(1)).getOrCreate().sparkContext;
    kanonymize(args(0), args(1), args(2), args(3).toInt);
    val t1 = System.nanoTime()

    println("Time Taken: " + ((t1 - t0) / 1000000));

    val linesRDDOP = new DataReader().readDataFile(sc, args(2), 40).cache();
    val totalIL = linesRDDOP.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));

  }
  
  var rdds: List[RDD[(Long, scala.collection.mutable.Map[Int, String])]] = List();
  var discernabilityMetric: Double = 0;
  var metadataFilePath: String = null;
  var dataReader: DataReader = null;
  var sc: SparkContext = null;

  /**
   * Using following singleton to retrieve/broadcast metadata variables.
   */

  /**
   * This is implementation of k-anonymize function that finds dimension, paritions recursively.
   */
  def kanonymize(hdfsDataFilePath: String, scPath: String, outputFilePath: String, k: Int) {

    val dataReader = new DataReader();
    sc.setLogLevel("ERROR");
    val linesRDD = dataReader.readDataFile(sc, hdfsDataFilePath, 20);

    val metadata = LBSMetadata.getInstance();
    /**
     * First k-anonymity call.
     */
    kanonymize(linesRDD, k)
    writeOutputToFile(rdds, outputFilePath);
    sc.stop();
  }
  /**
   * This method unites summary statistic and equivalence class and outputs the csv file on given path.
   */
  def writeOutputToFile(rdds: List[RDD[(Long, scala.collection.mutable.Map[Int, String])]], filePath: String) {

    /**
     * Merge individual RDDs
     */
    val rddsMerged = sc.union(rdds).sortBy(_._1);
    new DataWriter(sc).writeRDDToAFile(filePath, rddsMerged.map({ case (x, y) => (x, y.toArray.sortBy(_._1).map(_._2).mkString(",")) }).sortBy(_._1).map(_._2));

    val totalIL = rddsMerged.map(_._2).map(x => InfoLossCalculator.IL(x)).mean();
    println("Total IL " + 100 * (totalIL / InfoLossCalculator.getMaximulInformationLoss()) + " Benefit with no attack: " + 100 * (1 - (totalIL / InfoLossCalculator.getMaximulInformationLoss())));

  }

  /**
   * Cdm = Sum(|E|*|E|)
   */
  def getDiscernabilityMetric(): Double = {
    return discernabilityMetric;
  }

  /**
   * This function finds dimension, performs cut based on the median value and
   */
  def kanonymize(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], k: Int) {

    linesRDD.cache();
    var leftRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
    var rightRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
    var leftPartitionedRange: String = null;
    var rightPartitionedRange: String = null;
    var metadata = LBSMetadata.getInstance();
    /**
     * Get the dimension for the cut.
     */
    var dimAndMedian: Dimensions = selectDimension(linesRDD, k);
    //  println("Dimension found  " + " : " + dimAndMedian.dimension() + " : "+dimAndMedian.tostring);
    if (dimAndMedian.dimension() >= 0) {

      if (metadata.getMetadata(dimAndMedian.dimension()).get.getColType() == 's') {
        leftRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.leftSet().contains(y.get(dimAndMedian.dimension()).get) } });
        rightRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.rightSet().contains(y.get(dimAndMedian.dimension()).get) } });
      } else {
        leftRDD = linesRDD.filter({ case (x, y) => y.get(dimAndMedian.dimension()).get.toDouble <= dimAndMedian.median().toDouble });
        rightRDD = linesRDD.filter({ case (x, y) => y.get(dimAndMedian.dimension()).get.toDouble > dimAndMedian.median().toDouble });
      }
      var leftSize = leftRDD.count();
      var rightSize = rightRDD.count();
      linesRDD.unpersist(true);
      if (leftSize >= k && rightSize >= k) {
        /**
         * Add the range value applicable to all left set elements
         */
        if (leftSize == k) {
          assignSummaryStatisticAndAddToList(leftRDD);
        } else {
          kanonymize(leftRDD, k);
        }
        if (rightSize == k) {
          assignSummaryStatisticAndAddToList(rightRDD);
        } else {
          kanonymize(rightRDD, k);
        }

      } else {
        //println("No cut [" + "](" + leftSize + ") : : (" + rightSize + ")");
        assignSummaryStatisticAndAddToList(linesRDD);
      }
    } else {
      /**
       * Negative dimension means that we are out of records after Quasi-identifiers have been filtered.
       */
      assignSummaryStatisticAndAddToList(linesRDD);
    }
  }
  /**
   * This method computes Range statistic for dimensions on which cut has not been performed yet.
   */
  def assignSummaryStatisticAndAddToList(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])]) {

    var metadata = LBSMetadata.getInstance();

    var indexValuePairs = linesRDD.flatMap({ case (index1, map) => (map) });

    var indexValueGroupedIntermediate = indexValuePairs.groupByKey().map({ case (index, list) => (index, list.toList.distinct) }).filter(_._1 < metadata.numColumns())

    var indexValueGrouped = indexValueGroupedIntermediate.map({
      case (x, y) =>
        var column = metadata.getMetadata(x).get;
        if (column.getIsQuasiIdentifier()) {
          if (column.getColType() == 's') {
            (x, column.findCategory(y.toArray).value());
          } else {
            var listOfNumbers = y.map(_.toDouble);
            if (listOfNumbers.min == listOfNumbers.max) {
              (x, listOfNumbers.min.toString);
            } else {
              (x, listOfNumbers.min + "_" + listOfNumbers.max);
            }
          }
        } else {
          (-1, "")
        }
    });

    var map: scala.collection.Map[Int, String] = indexValueGrouped.collectAsMap();
    var rdd = linesRDD.map({
      case (x, y) =>

        for (i <- 0 to metadata.numColumns() - 1) {

          y.put(i, map.get(i).get);
        }
        (x, y)
    });
    rdds = rdds :+ sc.parallelize(rdd.collect());

  }

  /**
   * Accept RDD containing row numbers and column values along with their index.
   */
  def selectDimension(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], k: Int): Dimensions = {

    try {
      linesRDD.cache();
      val metadata = LBSMetadata.getInstance();
      /**
       * Remove row IDs.
       */

      val indexValuePairs = linesRDD.flatMap({ case (index, map) => (map) });

      /**
       * Filter all columns that are blocked in order to prevent unnecessary shuffling.
       */
      val filteredColumns = indexValuePairs /*.filter({ case (index, value) => !blockedIndices.contains(index) })*/ ;
      /**
       * Group values by index, make it a distinct list, cache it on executors.
       */
      val indexValueGrouped = filteredColumns.groupByKey().map({ case (index, list) => (index, list.toList) }).cache();
      /**
       * get size of the group for each index.
       */
      val indexAndCount = indexValueGrouped.map({ case (index, list) => (index, list.distinct.size) })

      /**
       * sort by second column, (ASC=false), .
       */
      val sortedIndexAndCount = indexAndCount.sortBy(_._2, false);
      /**
       * We are using range statistic, if column selected is numeric, we need maximum number of unique values, for string column, we need to see generalization hierarchy.
       */
      /**
       * Take first entry, first value.
       */
      val dimToBeReturned: Int = sortedIndexAndCount.first()._1;
      /**
       * Find the exact list for selected dimension, sort list of values, extract middle element
       */

      if (metadata.getMetadata(dimToBeReturned).get.getColType() == 's') {
        // distribute keys by putting alternate ones in alternate list. This way two partition sizes should roughly be near each other

        val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).map(x => (x, 1)).reduceByKey((a, b) => a + b).sortByKey(true).collect();

        val total = sortedListOfValues.map(_._2).sum;
        var runningSum = 0;
        var leftList = List[String]();
        var rightList = List[String]();
        for (pair <- sortedListOfValues) {
          if (runningSum <= total / 2) {
            leftList = leftList.::(pair._1);
            runningSum = runningSum + pair._2
          } else {
            rightList = rightList.::(pair._1);
          }
        }
        linesRDD.unpersist(true);
        return new Dimensions(dimToBeReturned, 0, 0, 0, leftList.toArray, rightList.toArray);

      } else {
        val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).sortBy(x => x.toDouble).zipWithIndex();
        /**
         * Create reverseIndex so that lookup using index becomes possible as we are interested in the "median" value.
         */
        val reverseIndex = sortedListOfValues.map({ case (x, y) => (y, x) });

        val min = reverseIndex.lookup(0)(0).toDouble;
        val median = reverseIndex.lookup((sortedListOfValues.count() / 2))(0).toDouble;
        val max = reverseIndex.lookup(sortedListOfValues.count() - 1)(0).toDouble;

        /**
         * return the tuple.
         */

        linesRDD.unpersist(true);
        return new Dimensions(dimToBeReturned, min, median, max, null, null);
      }
    } catch {
      case s: Exception =>
        println("Condition occured: " + s.getMessage);
        return new Dimensions(-1, -1, -1, -1, null, null);
    }
  }
}