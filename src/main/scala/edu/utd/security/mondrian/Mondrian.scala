package edu.utd.security.mondrian

import scala.collection.mutable.ListBuffer
import scala.xml.Node
import scala.xml.NodeSeq
import scala.xml.XML

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Mondrian {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]");
  var sc: SparkContext = SparkContext.getOrCreate(conf);

  def getSC(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext(conf)
    }
    sc;
  }

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR");
    kanonymize(args(0),args(1),args(2),4);
  }
  var numColumns = 0;
  var metadata = new Metadata(Map());
  var rdds: List[RDD[(Long, Array[(String, Int)])]] = List();
  var summaryStatistics: List[RDD[(Long, scala.collection.Map[Int, String])]] = List();
  def kanonymize(hdfsDataPath:String ,metadataFilePath :String, outputFilePath:String, k: Int) {

    val file = sc.textFile(hdfsDataPath)
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
    val linesRDD = linesWithIndex.map({ case (value, index) => (index, value.split(",").zipWithIndex) })

    /**
     * First k-anonymity call.
     */
    linesRDD.cache();
    metadata = readMetadata(metadataFilePath);

    sc.broadcast(metadata);
    numColumns = metadata.numColumns();

    var blockedIndices: scala.collection.mutable.Set[Int] = scala.collection.mutable.Set(); ;
    for (i <- 0 to numColumns - 1) {
      blockedIndices += (numColumns + i)
    }
    println(blockedIndices.mkString(","));

    for (i <- 0 to numColumns - 1) {
      if (!metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
        blockedIndices += i;
      }
    }
    val k = 4;
    kanonymize(linesRDD, blockedIndices, metadata, k)

    println("Cavg found: " + getNormalizedAverageEquiValenceClassSizeMetric(linesRDD, k));
    writeOutputToFile(rdds, outputFilePath);

  }
  /**
   * This method unites summary statistic and equivalence class and outputs the csv file on given path.
   */
  def writeOutputToFile(rdds: List[RDD[(Long, Array[(String, Int)])]], filePath: String) {
    
    /**
     * Merge individual RDDs
     */
    val rddsMerged = sc.union(rdds).sortBy(_._1);
    /**
     * Unite rdds with summaryStatistics
     */
    println("Total map size:" + sc.union(summaryStatistics).collect().length)
    val rddString = sc.union(summaryStatistics).join(rddsMerged).map({
      case (rowIndex, (summaryMap, data)) => {
        var sb: StringBuilder = new StringBuilder();
        var map: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]();
        /**
         * Accumulate column values in a map for easy retrieval.
         */
        data.foreach(columnIndexValuePair => {
          map += ((columnIndexValuePair._2, columnIndexValuePair._1));
        })
        /**
         * Append column values to sb.
         */
        for (i <- 0 to metadata.numColumns() - 1) {
          if (map.get((metadata.numColumns() + i)) == None) {
            if (metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
              /**
               * Summary statistic for the quasi-identifier without any cut on current column.
               */
              sb.append(summaryMap.get(i).get);
            } else {
              /**
               * Default output plain value since it is non-quasi ID field.
               */
              sb.append(map.get(i).get)
            }
          } else {
            /**
             * Paritioned value
             */
            sb.append(map.get((metadata.numColumns() + i)).get);
          }
          if (i != data.length - 1) {
            sb.append(",");
          }
        }
        sb.toString();
      }
    });
    /**
     * Use coalese to prevent output being written to multiple partition files.
     */
    rddString.coalesce(1, true).saveAsTextFile(filePath)
  }
  /**
   * Cavg = (Total_recods/total_equivalence_classes)/k
   */
  def getNormalizedAverageEquiValenceClassSizeMetric(linesRDD: RDD[(Long, Array[(String, Int)])], k: Int): Double = {

    return (linesRDD.count / summaryStatistics.length) / k;
  }
  
  /**
   * Cdm = Sum(|E|*|E|)
   */
  def getDiscernabilityMetric(linesRDD: RDD[(Long, Array[(String, Int)])], k: Int): Double = {
    return summaryStatistics.map(x => Math.pow(x.count(), 2)).reduce(_ + _);
  }

  /**
   * This method reads metadata object from an xml file.
   */
  def readMetadata(filePath: String): Metadata =
    {
      var columns: Map[Int, Column] = Map[Int, Column]();
      val xml = XML.loadFile(filePath);
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
  def kanonymize(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: scala.collection.mutable.Set[Int], metadata: Metadata, k: Int) {
    val count = linesRDD.count();
    println("First call" + count);
    if (count < k) {
      println("Invalid cut : Cluster is already smaller than value of K")
      /**
       * This condition should never happen., if it happens, exit.
       */
      System.exit(0);
    } else if (count == k) {
      assignSummaryStatisticAndAddToList(linesRDD);
      println("Cannot perform cut. Cluster is exactly of size of K");
    } else {
      val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices, metadata, k);
      sc.broadcast(dimAndMedian);
      println(" =>" + dimAndMedian.dimension() + " (" + dimAndMedian.min() + "-" + dimAndMedian.median() + "-" + dimAndMedian.max());
      blockedIndices.+(dimAndMedian.dimension());
      var blockedIndices1: scala.collection.mutable.Set[Int] = blockedIndices.clone();
      var blockedIndices2: scala.collection.mutable.Set[Int] = blockedIndices.clone();

      val sortedRDD = linesRDD.sortBy({ case (x, y) => y(dimAndMedian.dimension())._1 }, true);

      if (metadata.getMetadata(dimAndMedian.dimension()).get.getColType() == 's') {
        val leftRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.leftSet().contains(y(dimAndMedian.dimension())._1.trim()) } });
        val rightRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.rightSet().contains(y(dimAndMedian.dimension())._1.trim()) } });
        val leftSize = leftRDD.count();
        val rightSize = rightRDD.count();

        println(dimAndMedian.leftSet().mkString + " -- " + dimAndMedian.rightSet().mkString + "==>" + leftSize + ":" + rightSize);
        if (leftSize >= k && rightSize >= k) {

          val leftCategory: Category = metadata.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.leftSet())
          val rightCategory: Category = metadata.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.rightSet())

          val leftRDDWithRange = getNumericRDDWithRange(leftRDD, dimAndMedian.dimension(), leftCategory.value().trim());
          val rightRDDWithRange = getNumericRDDWithRange(rightRDD, dimAndMedian.dimension(), rightCategory.value().trim());
          /**
           * Add the range value applicable to all left set elements
           */
          println("Making the cut on " + dimAndMedian.dimension() + " " + leftCategory.value().trim() + " : " + rightCategory.value().trim());

          kanonymize(leftRDDWithRange, blockedIndices1, metadata, k);
          kanonymize(rightRDDWithRange, blockedIndices2, metadata, k);
        } else {
          assignSummaryStatisticAndAddToList(linesRDD);
        }
      } else {
        println(dimAndMedian.dimension() + "-->" + metadata.getMetadata(dimAndMedian.dimension()).toString());
        val leftRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble <= dimAndMedian.median().toDouble });
        val rightRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble > dimAndMedian.median().toDouble });
        val leftSize = leftRDD.count();
        val rightSize = rightRDD.count();
        if (leftSize >= k && rightSize >= k) {

          /**
           * Add the range value applicable to all left set elements
           */
          println("Making the cut on " + dimAndMedian.dimension());

          val leftRDDWithRange = getNumericRDDWithRange(leftRDD, dimAndMedian.dimension(), dimAndMedian.min() + "-" + dimAndMedian.median());
          val rightRDDWithRange = getNumericRDDWithRange(rightRDD, dimAndMedian.dimension(), dimAndMedian.median() + "-" + dimAndMedian.max());
          kanonymize(leftRDDWithRange, blockedIndices1, metadata, k);
          kanonymize(rightRDDWithRange, blockedIndices2, metadata, k);
        } else {
          assignSummaryStatisticAndAddToList(linesRDD);
        }
      }
    }

  }
  /**
   * This method computes Range statistic for dimensions on which cut has not performed yet.
   */
  def assignSummaryStatisticAndAddToList(linesRDD: RDD[(Long, Array[(String, Int)])]) {

    rdds = rdds :+ linesRDD;
    val indexValuePairs = linesRDD.flatMap({ case (index1, list) => (list) }).map({
      case (value, index) => (index, value.trim())
    });
    val indexValueGrouped = indexValuePairs.groupByKey().map({ case (index, list) => (index, list.toList.distinct) }).filter(_._1 < metadata.numColumns()).map({
      case (x, y) =>
        val column = metadata.getMetadata(x).get;
        if (column.getColType() == 's') {
          (x,  column.findCategory(y.toArray).value());
        } else {
          val listOfNumbers = y.map(_.toDouble);
          (x, listOfNumbers.min + "-" + listOfNumbers.max);
        }
    });

    var map:  scala.collection.Map[Int, String] = indexValueGrouped.collectAsMap();
    

    summaryStatistics = summaryStatistics:+linesRDD.keys.map { x => (x, map) }; ;
  }

  def getNumericRDDWithRange(rightRDD: RDD[(Long, Array[(String, Int)])], dimension: Int, newValue: String): RDD[(Long, Array[(String, Int)])] =
    {
      val rightRDDWithRange = rightRDD.map({
        case (value, linesArray) => (value, {
          var index = -1;
          for (i <- 0 to linesArray.length - 1) {
            if (linesArray(i)._2 == (metadata.numColumns() + dimension)) {
              index = i;
            }
          }
          var newArray: ListBuffer[(String, Int)] = new ListBuffer();
          newArray ++= linesArray;
          if (index == -1) {
            newArray += new Tuple2(newValue, (metadata.numColumns() + dimension));
          } else {
            newArray.update(index, new Tuple2(newValue, (metadata.numColumns() + dimension)));
          }
          newArray.toArray
        })
      })
      return rightRDDWithRange;
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
  def selectDimension(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: scala.collection.mutable.Set[Int], metadata: Metadata, k: Int): Dimensions = {

    sc.broadcast(blockedIndices);
    /**
     * Remove row IDs.
     */

    val valueIndexPairs = linesRDD.flatMap({ case (index1, list) => (list) });
    /**
     * reverse the pair
     */
    val indexValuePairs = valueIndexPairs.map({
      case (value, index) => (index, value.trim())
    });

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

      val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).map(x => (x.trim(), 1)).reduceByKey((a, b) => a + b).sortByKey(false);
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
      val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).sortBy(x => x.trim().toDouble).zipWithIndex();
      /**
       * Create reverseIndex so that lookup using index becomes possible as we are interested in the "median" value.
       */
      val reverseIndex = sortedListOfValues.map({ case (x, y) => (y, x) });

      val min = reverseIndex.lookup(0)(0).trim().toDouble;
      val median = reverseIndex.lookup((sortedListOfValues.count() / 2))(0).trim().toDouble;
      val max = reverseIndex.lookup(sortedListOfValues.count() - 1)(0).trim().toDouble;

      /**
       * return the tuple.
       */
      return new Dimensions(dimToBeReturned, min, median, max, null, null);
    }
  }
}