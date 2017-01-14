package edu.utd.security.mondrian

import scala.xml.Node
import scala.xml.NodeSeq
import scala.xml.XML

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * This is implementation of paper on Mondrian multi-dimensional paritioning for K-Anonymity
 * Paper authors = {Kristen Lefevre and David J. Dewitt and Raghu Ramakrishnan},
 * Paper title = {Mondrian multidimensional k-anonymity},
 * booktitle = {In ICDE},
 * year = {2006}
 */
object Mondrian {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]");
  var sc: SparkContext = SparkContext.getOrCreate(conf);

  def getSC(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext(conf)
    }
    sc;
  }

  /**
   * This method accepts 4 parameters -
   * 1. hdfs File path
   * 2. Metadata file
   * 3. Output File path
   * 4. value of k
   */
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR");
    kanonymize(args(0), args(1), args(2), args(3).toInt);
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
  var numColumns = 0;
  var metadata = new Metadata(Map());
  var rdds: List[RDD[(Long, scala.collection.mutable.Map[Int,String])]] = List();
  var summaryStatistics: List[RDD[(Long, scala.collection.Map[Int, String])]] = List();
  var discernabilityMetric: Double = 0;
  
  /**
   * This is implementation of k-anonymize function that finds dimension, paritions recursively.
   */
  def kanonymize(hdfsDataPath: String, metadataFilePath: String, outputFilePath: String, k: Int) {

    val file = sc.textFile(hdfsDataPath, 8)
    /**
     * Split by new line, filter lines containing missing data.
     */
    val lines = file.flatMap(_.split("\n")).filter { !_.contains("?") };
    /**
     * Retain indices of lines
     */
    val linesWithIndex = lines.zipWithIndex
    /**
     * split columns inside each line, zip with index.
     */
    val linesRDD = linesWithIndex.map({ case (value, index) => (index, value.split(",").zipWithIndex) }).map({
      case (index, data) =>
        var map: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]();
        data.foreach(columnIndexValuePair => {
          map += ((columnIndexValuePair._2, columnIndexValuePair._1.trim()));
        })
        (index,map);
    })

    /**
     * First k-anonymity call.
     */
    linesRDD.cache();
    metadata = readMetadata(metadataFilePath);

    sc.broadcast(metadata);
    numColumns = metadata.numColumns();
    /**
     * all column indices that would contain newer values need to be blocked from partitioning logic
     * along with non-QuasiIdentifier fields.
     */

    var blockedIndices: scala.collection.mutable.Set[Int] = scala.collection.mutable.Set(); ;
    for (i <- 0 to numColumns - 1) {
      blockedIndices += (numColumns + i)
      if (!metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
        blockedIndices += i;
      }
    }

    kanonymize(linesRDD, blockedIndices, metadata, k)

    println("Lines(with no missing values) Found: " + linesRDD.count());
    println("Cavg found: " + getNormalizedAverageEquiValenceClassSizeMetric(linesRDD, k));
    println("Cdm  found: " + getDiscernabilityMetric());
    writeOutputToFile(rdds, outputFilePath);

  }
  /**
   * This method unites summary statistic and equivalence class and outputs the csv file on given path.
   */
  def writeOutputToFile(rdds: List[RDD[(Long, scala.collection.mutable.Map[Int,String])]], filePath: String) {

    /**
     * Merge individual RDDs
     */
    val rddsMerged = sc.union(rdds).sortBy(_._1);
    /**
     * Unite rdds with summaryStatistics
     */
    val rddString = sc.union(summaryStatistics).join(rddsMerged).map({
      case (rowIndex, (summaryMap, dataMap)) => {
        var sb: StringBuilder = new StringBuilder();
  
        /**
         * Append column values to sb.
         */
        for (i <- 0 to metadata.numColumns() - 1) {
          if (dataMap.get((metadata.numColumns() + i)) == None) {
            if (metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
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
            sb.append(dataMap.get((metadata.numColumns() + i)).get);
          }
          if (i != metadata.numColumns()- 1) {
            sb.append(",");
          }
        }
        sb.toString();
      }
    });
    /**
     * Use coalese to prevent output being written to multiple partition files.
     */
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(filePath), sc.hadoopConfiguration)
    if (fs.exists(new org.apache.hadoop.fs.Path(filePath))) {
      fs.delete(new org.apache.hadoop.fs.Path(filePath));
    }

    println("Total Unique Equivalence classes found: " + summaryStatistics.length);
    rddString.coalesce(1, true).saveAsTextFile(filePath)
    println("Output written to file: " + filePath);
  }
  /**
   * Cavg = (Total_recods/total_equivalence_classes)/k
   */
  def getNormalizedAverageEquiValenceClassSizeMetric(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int,String])], k: Int): Double = {

    return ((linesRDD.count.doubleValue() / summaryStatistics.length) / k);
  }

  /**
   * Cdm = Sum(|E|*|E|)
   */
  def getDiscernabilityMetric(): Double = {
    return discernabilityMetric;
  }

  /**
   * This method reads metadata object from an xml file.
   */
  def readMetadata(filePath: String): Metadata =
    {
      var columns: Map[Int, Column] = Map[Int, Column]();
      val xml = XML.loadString(sc.textFile(filePath).toLocalIterator.mkString);
      val iterator = xml.\\("columns").\("column").iterator;
      while (iterator.hasNext) {
        val node = iterator.next;
        if (node.\("type").text.charAt(0) == 's') {
          /**
           * For String column types.
           */
          if (node.\("hierarchy").text.length() > 0) {
            val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, getHierarchy(node.\("hierarchy"), "*"));
            columns += ((column.getIndex(), column));
          } else {
            val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, new Category("*"));
            columns += ((column.getIndex(), column));
          }
        } else {
          /**
           * Numeric columns.
           */
          val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, null);
          columns += ((column.getIndex(), column));
        }
      }
      return new Metadata(columns);
    }

  /**
   * This method accepts a categorical node sequence and parses entire generalization hierarchy and returns the root.
   */
  def getHierarchy(node: NodeSeq, name: String): Category = {

    var category = new Category(name);
    node.\("children").foreach { x =>
      {
        category.addChildren(getHierarchy(x, x.\("value").text));
      }
    };
    return category;
  }
  /**
   * This function finds dimension, performs cut based on the median value and
   */
  def kanonymize(linesRDD: RDD[(Long, scala.collection.mutable.Map[Int, String])], blockedIndices: scala.collection.mutable.Set[Int], metadata: Metadata, k: Int) {

    var leftRDD: RDD[(Long, scala.collection.mutable.Map[ Int,String])] = null;
    var rightRDD: RDD[(Long, scala.collection.mutable.Map[Int,String])] = null;
    var leftPartitionedRange: String = null;
    var rightPartitionedRange: String = null;

    /**
     * Get the dimension for the cut.
     */
    val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices, metadata, k);

    if (dimAndMedian.dimension() >= 0) {
      sc.broadcast(dimAndMedian);
      var blockedIndices1: scala.collection.mutable.Set[Int] = blockedIndices.+(dimAndMedian.dimension()).clone();
      var blockedIndices2: scala.collection.mutable.Set[Int] = blockedIndices.+(dimAndMedian.dimension()).clone();

      val sortedRDD = linesRDD.sortBy({ case (x, y) => y.get(dimAndMedian.dimension())}, true);

      if (metadata.getMetadata(dimAndMedian.dimension()).get.getColType() == 's') {
        leftRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.leftSet().contains(y.get(dimAndMedian.dimension()).get) } });
        rightRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.rightSet().contains(y.get(dimAndMedian.dimension()).get) } });
        leftPartitionedRange = metadata.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.leftSet()).value();
        rightPartitionedRange = metadata.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.rightSet()).value();
      } else {
        leftRDD = linesRDD.filter({ case (x, y) => y.get(dimAndMedian.dimension()).get.toDouble <= dimAndMedian.median().toDouble });
        rightRDD = linesRDD.filter({ case (x, y) => y.get(dimAndMedian.dimension()).get.toDouble > dimAndMedian.median().toDouble });
        leftPartitionedRange =if(dimAndMedian.min().equals( dimAndMedian.median())) dimAndMedian.min().toString() else dimAndMedian.min() + "-" + dimAndMedian.median();
        rightPartitionedRange = if(dimAndMedian.median().equals(dimAndMedian.max())) dimAndMedian.median().toString() else dimAndMedian.median() + "-" + dimAndMedian.max();
      }
      val leftSize = leftRDD.count();
      val rightSize = rightRDD.count();
      if (leftSize >= k && rightSize >= k) {
     
        val leftRDDWithRange = partitionRDD(leftRDD, dimAndMedian.dimension(), leftPartitionedRange);
        val rightRDDWithRange = partitionRDD(rightRDD, dimAndMedian.dimension(), rightPartitionedRange);
        /**
         * Add the range value applicable to all left set elements
         */
        println("Making the cut on dimension[" +metadata.getMetadata(dimAndMedian.dimension()).get.getName() + "] =>[ " + leftPartitionedRange + "] : [" + rightPartitionedRange + "]");
        if (leftSize == k) {
          assignSummaryStatisticAndAddToList(leftRDDWithRange);
        } else {
          kanonymize(leftRDDWithRange, blockedIndices1, metadata, k);
        }

        if (rightSize == k) {
          assignSummaryStatisticAndAddToList(rightRDDWithRange);
        } else {
          kanonymize(rightRDDWithRange, blockedIndices2, metadata, k);
        }
      } else {
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
   * This method computes Range statistic for dimensions on which cut has not performed yet.
   */
  def assignSummaryStatisticAndAddToList(linesRDD: RDD[(Long, scala.collection.mutable.Map[ Int,String])]) {

    rdds = rdds :+ linesRDD;
    val indexValuePairs = linesRDD.flatMap({ case (index1, map) => (map) });
    val indexValueGrouped = indexValuePairs.groupByKey().map({ case (index, list) => (index, list.toList.distinct) }).filter(_._1 < metadata.numColumns()).map({
      case (x, y) =>
        val column = metadata.getMetadata(x).get;
        if (column.getIsQuasiIdentifier()) {
          if (column.getColType() == 's') {
            (x, column.findCategory(y.toArray).value());
          } else {
            val listOfNumbers = y.map(_.toDouble);
            (x, listOfNumbers.min + "-" + listOfNumbers.max);
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
  def partitionRDD(inputRDD: RDD[(Long, scala.collection.mutable.Map[Int,String])], dimension: Int, newValue: String): RDD[(Long, scala.collection.mutable.Map[ Int,String])] =
    {
      val newRDD = inputRDD.map({
        case (value, indexMap) => (value, {
         indexMap.put((metadata.numColumns() + dimension),newValue);
         indexMap
        })
      })
      return newRDD;
    }

  /**
   * Class responsible for holding metadata of columns.
   */
  class Metadata(columnMetadata: Map[Int, Column]) extends Serializable {

    def getMetadata(columnId: Int): Option[Column] = {
      return columnMetadata.get(columnId);
    }
    def numColumns(): Int = {
      return columnMetadata.size;
    }
    override def toString: String = {
      return columnMetadata.mkString;
    }
  }

  /**
   * Accept RDD containing row numbers and column values along with their index.
   */
  def selectDimension(linesRDD: RDD[(Long, scala.collection.mutable.Map[ Int,String])], blockedIndices: scala.collection.mutable.Set[Int], metadata: Metadata, k: Int): Dimensions = {

    try {
      sc.broadcast(blockedIndices);
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

      if (metadata.getMetadata(dimToBeReturned).get.getColType() == 's') {
        // distribute keys by putting alternate ones in alternate list. This way two partition sizes should roughly be near each other

        val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).map(x => (x, 1)).reduceByKey((a, b) => a + b).sortByKey(false);
        val firstValue = sortedListOfValues.first();
        if (sortedListOfValues.first()._2 > k) {
          var leftList = Array(firstValue._1);
          var rightList = sortedListOfValues.filter { case (x, y) => !x.equals(leftList) }.keys.collect();
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
      case s: Exception => println("Condition occured: " + s.getMessage);
    }

    return new Dimensions(-1, -1, -1, -1, null, null);
  }
}