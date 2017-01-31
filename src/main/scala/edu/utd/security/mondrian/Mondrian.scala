package edu.utd.security.mondrian

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

import edu.utd.security.common.Metadata


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
   
   * Cluster execution command -  
   *  ./spark-submit --class edu.utd.security.mondrian.Mondrian --master "spark://cloudmaster3:7077"  /data/kanchan/blueray-0.0.18-$BLUEVAR-SNAPSHOT.jar  "hdfs://cloudmaster3:54310/user/adult.data.txt" "hdfs://cloudmaster3:54310/user/metadata.xml" "hdfs://cloudmaster3:54310/user/small_data_500" 500
   * 
   * */
  def main(args: Array[String]): Unit = {
   // sc.setLogLevel("ERROR");
   // kanonymize(args(0), args(1), args(2), args(3).toInt);
  }
  /**
   * Program invariants
   * numColumns = Number of columns present in the data
   * metadata = Parsed metadata object
   * rdds = List of Equivalence Class RDDs
   * summaryStatistic = Summary statistic for each partition.
   * totalEquivalenceClasses = total number of equivalence classes found
   * discernabilityMetric = metric calculated.
   */

  var rdds: List[RDD[(Long, scala.collection.mutable.Map[Int, String])]] = List();
  var summaryStatistics: List[RDD[(Long, scala.collection.Map[Int, String])]] = List();
  var discernabilityMetric: Double = 0;
  var metadataFilePath: String = null;
  var dataReader: DataReader = null;
  var sc: SparkContext = SparkSession
    .builder.appName("Mondrian").master("local[2]").getOrCreate().sparkContext;

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

  /**
   * This is implementation of k-anonymize function that finds dimension, paritions recursively.
   */
  def kanonymize(hdfsDataFilePath: String, metadataFilePath: String, outputFilePath: String, k: Int) {

    val dataReader = new DataReader(sc);

    val linesRDD = dataReader.readDataFile(hdfsDataFilePath, true);
    linesRDD.cache();
    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    /**
     * all column indices that would contain newer values need to be blocked from partitioning logic
     * along with non-QuasiIdentifier fields.
     */

    var blockedIndices: scala.collection.mutable.Set[Int] = scala.collection.mutable.Set(); ;
    for (i <- 0 to metadata.value.numColumns() - 1) {
      blockedIndices += (metadata.value.numColumns() + i)
      if (!metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
        blockedIndices += i;
      }
    }
    /**
     * First k-anonymity call.
     */
    kanonymize(linesRDD, blockedIndices, k)

    println("Lines(with no missing values) Found: " + linesRDD.count());
    println("Cavg found: " + getNormalizedAverageEquiValenceClassSizeMetric(linesRDD, k));
    println("Cdm  found: " + getDiscernabilityMetric());
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
    /**
     * Unite rdds with summaryStatistics
     */
    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    val rdd = sc.union(summaryStatistics).join(rddsMerged).map({
      case (rowIndex, (summaryMap, dataMap)) => {
        var sb: StringBuilder = new StringBuilder();

        /**
         * Append column values to sb.
         */
        for (i <- 0 to metadata.value.numColumns() - 1) {
          if (dataMap.get((metadata.value.numColumns() + i)) == None) {
            if (metadata.value.getMetadata(i).get.getIsQuasiIdentifier()) {
              /**
               * Summary statistic for the quasi-identifier without any cut on current column.
               */
              sb.append(summaryMap.get(i).get);
            } else {
              /**
               * Default output plain value since it is non-quasi ID field.
               */
              sb.append(dataMap.get(i).get)
            }
          } else {
            /**
             * Paritioned value
             */
            sb.append(dataMap.get((metadata.value.numColumns() + i)).get);
          }
          if (i != metadata.value.numColumns() - 1) {
            sb.append(",");
          }
        }
        sb.toString();
      }
    });

    println("Total Unique Equivalence classes found: " + summaryStatistics.length);
    new DataWriter(sc).writeRDDToAFile(filePath, rdd);
  }
  /**
   * Cavg = (Total_recods/total_equivalence_classes)/k
   */
  def getNormalizedAverageEquiValenceClassSizeMetric(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], k: Int): Double = {

    return ((linesRDD.count.doubleValue() / summaryStatistics.length) / k);
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
  def kanonymize(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], blockedIndices: scala.collection.mutable.Set[Int], k: Int) {

    var leftRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
    var rightRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])] = null;
    var leftPartitionedRange: String = null;
    var rightPartitionedRange: String = null;
    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    /**
     * Get the dimension for the cut.
     */
    val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices, k);
    println("Dimension found for: "+linesRDD.count()+" : " + dimAndMedian.dimension()+" : ");
    if (dimAndMedian.dimension() >= 0) {
      var blockedIndices1: scala.collection.mutable.Set[Int] = blockedIndices.+(dimAndMedian.dimension()).clone();
      var blockedIndices2: scala.collection.mutable.Set[Int] = blockedIndices.+(dimAndMedian.dimension()).clone();

      val sortedRDD = linesRDD.sortBy({ case (x, y) => y.get(dimAndMedian.dimension()) }, true);

      if (metadata.value.getMetadata(dimAndMedian.dimension()).get.getColType() == 's') {
        leftRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.leftSet().contains(y.get(dimAndMedian.dimension()).get) } });
        rightRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.rightSet().contains(y.get(dimAndMedian.dimension()).get) } });
        leftPartitionedRange = metadata.value.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.leftSet()).value();
        rightPartitionedRange = metadata.value.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.rightSet()).value();
      } else {
        leftRDD = linesRDD.filter({ case (x, y) => y.get(dimAndMedian.dimension()).get.toDouble <= dimAndMedian.median().toDouble });
        rightRDD = linesRDD.filter({ case (x, y) => y.get(dimAndMedian.dimension()).get.toDouble > dimAndMedian.median().toDouble });
        leftPartitionedRange = if (dimAndMedian.min().equals(dimAndMedian.median())) dimAndMedian.min().toString() else dimAndMedian.min() + "-" + dimAndMedian.median();
        rightPartitionedRange = if (dimAndMedian.median().equals(dimAndMedian.max())) dimAndMedian.median().toString() else dimAndMedian.median() + "-" + dimAndMedian.max();
      }
      val leftSize = leftRDD.count();
      val rightSize = rightRDD.count();
      if (leftSize >= k && rightSize >= k) {

        println("Making the cut on dimension[" + metadata.value.getMetadata(dimAndMedian.dimension()).get.getName() + "](" + leftSize + ") [ " + leftPartitionedRange + "] :::: [" + rightPartitionedRange + "](" + rightSize + ")");

        val leftRDDWithRange = partitionRDD(leftRDD, dimAndMedian.dimension(), leftPartitionedRange);
        val rightRDDWithRange = partitionRDD(rightRDD, dimAndMedian.dimension(), rightPartitionedRange);
        /**
         * Add the range value applicable to all left set elements
         */
        if (leftSize == k) {
          assignSummaryStatisticAndAddToList(leftRDDWithRange);
        } else {
          kanonymize(leftRDDWithRange, blockedIndices1, k);
        }

        if (rightSize == k) {
          assignSummaryStatisticAndAddToList(rightRDDWithRange);
        } else {
          kanonymize(rightRDDWithRange, blockedIndices2, k);
        }
      } else {
          println("No cut [" + "](" + leftSize + ") : : (" + rightSize + ")");
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

    val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
    rdds = rdds :+ linesRDD;
    val indexValuePairs = linesRDD.flatMap({ case (index1, map) => (map) });

    val indexValueGroupedIntermediate = indexValuePairs.groupByKey().map({ case (index, list) => (index, list.toList.distinct) }).filter(_._1 < metadata.value.numColumns())

    val indexValueGrouped = indexValueGroupedIntermediate.map({
      case (x, y) =>
        val column = metadata.value.getMetadata(x).get;
        if (column.getIsQuasiIdentifier()) {
          if (column.getColType() == 's') {
            (x, column.findCategory(y.toArray).value());
          } else {
            val listOfNumbers = y.map(_.toDouble);
            if (listOfNumbers.min == listOfNumbers.max) {
              (x, listOfNumbers.min.toString);
            } else {
              (x, listOfNumbers.min + "-" + listOfNumbers.max);
            }
          }
        } else {
          (-1, "")
        }
    });

    var map: scala.collection.Map[Int, String] = indexValueGrouped.collectAsMap();

    val keys = linesRDD.keys;
    summaryStatistics = summaryStatistics :+ keys.map { x => (x, map) };
    discernabilityMetric += Math.pow(keys.count(), 2);
  }

  /**
   * This function updates RDD value at given index in entire RDD with the newValue.
   */
  def partitionRDD(inputRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], dimension: Int, newValue: String): RDD[(Long, scala.collection.mutable.Map[Int, String])] =
    {
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
      val newRDD = inputRDD.map({
        case (value, indexMap) => (value, {
          indexMap.put((metadata.value.numColumns() + dimension), newValue);
          indexMap
        })
      })
      return newRDD;
    }

  /**
   * Accept RDD containing row numbers and column values along with their index.
   */
  def selectDimension(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], blockedIndices: scala.collection.mutable.Set[Int], k: Int): Dimensions = {

    try {
      val metadata = Metadata.getInstance(sc, dataReader, metadataFilePath);
      /**
       * Remove row IDs.
       */

      val indexValuePairs = linesRDD.flatMap({ case (index, map) => (map) });

      /**
       * Filter all columns that are blocked in order to prevent unnecessary shuffling.
       */
      val filteredColumns = indexValuePairs.filter({ case (index, value) => !blockedIndices.contains(index) });
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

      if (metadata.value.getMetadata(dimToBeReturned).get.getColType() == 's') {
        // distribute keys by putting alternate ones in alternate list. This way two partition sizes should roughly be near each other

        val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).map(x => (x, 1)).reduceByKey((a, b) => a + b).sortByKey(false);
        val firstValue = sortedListOfValues.first();
        if (sortedListOfValues.first()._2 > k) {
          var leftList = Array(firstValue._1);
          var rightList = sortedListOfValues.filter { case (x, y) => !x.equals(firstValue._1) }.keys.collect();
          return new Dimensions(dimToBeReturned, 0, 0, 0, leftList, rightList);
        } else {
          val zippedListOfValues = sortedListOfValues.keys.zipWithIndex();
          var leftList = zippedListOfValues.filter { case (x, y) => y % 2 == 0 }.keys.collect();
          var rightList = zippedListOfValues.filter { case (x, y) => y % 2 == 1 }.keys.collect();
          return new Dimensions(dimToBeReturned, 0, 0, 0, leftList, rightList);

        }

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
        return new Dimensions(dimToBeReturned, min, median, max, null, null);
      }
    } catch {
      case s: Exception =>
        println("Condition occured: " + s.getMessage);
        return new Dimensions(-1, -1, -1, -1, null, null);
    }
  }
}