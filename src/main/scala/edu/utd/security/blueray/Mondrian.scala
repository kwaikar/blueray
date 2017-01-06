package edu.utd.security.blueray

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import org.apache.spark.broadcast.Broadcast

class Mondrian(filePath: String) extends Serializable {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
  var sc: SparkContext = SparkContext.getOrCreate(conf);

  def getSC(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext(conf)
    }
    sc;
  }

  def kanonymize(k: Int) {
    val file = sc.textFile("hdfs://localhost/user/adult.data.txt")
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
    val numColumns = linesRDD.take(1)(0)._2.length;
    sc.broadcast(numColumns);
    var blockedIndices: Set[Int] = Set();
    for (i <- 0 to numColumns - 1) {
      blockedIndices += (numColumns + i)
    }
    kanonymize(linesRDD, blockedIndices, k)
  }

  def kanonymize(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int], k: Int) {
    val count = linesRDD.count();
    println("First call" + count);
    if (count < k) {
      println("Invalid cut : Cluster is already smaller than value of K")
    } else if (count == k) {
      println("Cannot perform cut. Cluster is exactly of size of K");
    } else {
      val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices);
      sc.broadcast(dimAndMedian.dimension());
      sc.broadcast(dimAndMedian.median());
      sc.broadcast(dimAndMedian.min());
      sc.broadcast(dimAndMedian.max());
      println( " =>" + dimAndMedian.dimension() + " (" + dimAndMedian.min() + "-" + dimAndMedian.median() + "-" + dimAndMedian.max());

      val sortedRDD = linesRDD.sortBy({ case (x, y) => y(dimAndMedian.dimension())._1 }, true);
      println("--1"+sortedRDD.take(1))
      val leftRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble <= dimAndMedian.median().toDouble });
      println("--2"+leftRDD.take(1))
      val rightRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble > dimAndMedian.median().toDouble });
      println("--3"+rightRDD.take(1))
      val leftSize = leftRDD.count();
      val rightSize = rightRDD.count();
      blockedIndices.+(dimAndMedian.dimension());
   println(leftSize+"--"+rightSize+" "+blockedIndices)
      var blockedIndices1: Set[Int] = Set();
      blockedIndices1 ++= blockedIndices;
      var blockedIndices2: Set[Int] = Set();
      blockedIndices2 ++= blockedIndices;
      if (leftSize >= k && rightSize >= k) {

        /**
         * Add the range value applicable to all left set elements
         */
        println("Making the cut on " + dimAndMedian.dimension());

        val leftRDDWithRange = leftRDD.map({
          case (value, linesArray) => (value, {
            var index = 0;
            var arrayLegth = 1;
            for (i <- 0 to linesArray.length - 1) {
              if (linesArray(i)._2 == (numColumns + dimAndMedian.dimension())) {
                arrayLegth = 0;
                index = i;
              }
            }
            var newArray: Array[(String, Int)] = new Array(arrayLegth);
            newArray ++= linesArray;
            newArray.update(index, new Tuple2((dimAndMedian.min() + "_" + dimAndMedian.median()), (numColumns + dimAndMedian.dimension())))
            newArray;
          })
        });
        kanonymize(leftRDDWithRange, blockedIndices1, k);
        kanonymize(rightRDD, blockedIndices2, k);
      }
    }
  }

  /**
   * Inner class used for sharing output of dimension with the calling method.
   */
  class Dimensions(dimension: Int, min: String, median: String, max: String) extends Serializable {
    def dimension(): Int =
      {
        dimension
      }
    def min(): String =
      {
        min
      }
    def median(): String =
      {
        median
      }
    def max(): String =
      {
        max
      }
  }
  /**
   * Accept RDD containing row numbers and column values along with their index.
   */
  def selectDimension(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int]): Dimensions = {

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
    val indexValueGrouped = filteredColumns.groupByKey().map({ case (index, list) => (index, list.toList.distinct) }).cache();
    /**
     * get size of the group for each index.
     */
    val indexAndCount = indexValueGrouped.map({ case (index, list) => (index, list.size) })

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
    println("Dim : "+dimToBeReturned)
    /**
     * Find the exact list for selected dimension, sort list of values, extract middle element
     */
    val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).sortBy(x => x.trim()).zipWithIndex();
    /**
     * Create reverseIndex so that lookup using index becomes possible as we are interested in the "median" value.
     */
    val reverseIndex = sortedListOfValues.map({ case (x, y) => (y, x) });

    val min = reverseIndex.lookup(0)(0).trim();
    val median = reverseIndex.lookup((sortedListOfValues.count() / 2))(0).trim();
    val max = reverseIndex.lookup(sortedListOfValues.count() - 1)(0).trim();

    /**
     * return the tuple.
     */
    return new Dimensions(dimToBeReturned, min, median, max);
  }
}