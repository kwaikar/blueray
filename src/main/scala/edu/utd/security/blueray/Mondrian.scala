package edu.utd.security.blueray

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

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
    var blockedIndices: Set[Int] = Set(1, 2, 3, 5, 6, 7, 8, 9, 14, 13);
    for (i <- 0 to numColumns - 1) {
      blockedIndices += (numColumns + i)
    }
    val metadata:Metadata=readMetadata(filePath);
    val k=4;
    kanonymize(linesRDD, blockedIndices,metadata, k)
  }
def readMetadata(filePath:String):Metadata=
{
  var columns:Map[Int, Column] =Map[Int,Column]();
  
  
  var metadata :Metadata=  new Metadata(columns);
  return metadata
}
  /**
   * This function finds dimension, performs cut based on the median value and
   */
  def kanonymize(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int], metadata:Metadata, k: Int) {
    val count = linesRDD.count();
    println("First call" + count);
    if (count < k) {
      println("Invalid cut : Cluster is already smaller than value of K")
    } else if (count == k) {
      println("-------------->")

      linesRDD.take(1)(0)._2.foreach({ case (x, y) => println(x + "---" + y) });
      println("Cannot perform cut. Cluster is exactly of size of K");
    } else {
      val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices,metadata);
      sc.broadcast(dimAndMedian.dimension());
      sc.broadcast(dimAndMedian.median());
      sc.broadcast(dimAndMedian.min());
      sc.broadcast(dimAndMedian.max());
      println(" =>" + dimAndMedian.dimension() + " (" + dimAndMedian.min() + "-" + dimAndMedian.median() + "-" + dimAndMedian.max());

      val sortedRDD = linesRDD.sortBy({ case (x, y) => y(dimAndMedian.dimension())._1 }, true);
      val leftRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble <= dimAndMedian.median().toDouble });
      val rightRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble > dimAndMedian.median().toDouble });
      val leftSize = leftRDD.count();
      val rightSize = rightRDD.count();
      blockedIndices.+(dimAndMedian.dimension());
      println(leftSize + "--" + rightSize + " " + blockedIndices)
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
            var newArray: ListBuffer[(String, Int)] = ListBuffer();
            newArray ++= linesArray;
            if (arrayLegth == 1) {
              newArray += new Tuple2((dimAndMedian.min() + "_" + dimAndMedian.median()), (numColumns + dimAndMedian.dimension()));
            } else { newArray(index) = new Tuple2((dimAndMedian.min() + "_" + dimAndMedian.median()), (numColumns + dimAndMedian.dimension())); }
            newArray.toArray;
          })
        });

        val rightRDDWithRange = rightRDD.map({
          case (value, linesArray) => (value, {
            var index = 0;
            var arrayLegth = 1;
            for (i <- 0 to linesArray.length - 1) {
              if (linesArray(i)._2 == (numColumns + dimAndMedian.dimension())) {
                arrayLegth = 0;
                index = i;
              }
            }
            var newArray: ListBuffer[(String, Int)] = new ListBuffer();
            newArray ++= linesArray;
            if (arrayLegth == 1) {
              newArray += new Tuple2((dimAndMedian.median() + "_" + dimAndMedian.max()), (numColumns + dimAndMedian.dimension()));
            } else {
              newArray.update(index, new Tuple2((dimAndMedian.median() + "_" + dimAndMedian.max()), (numColumns + dimAndMedian.dimension())));
            }
            newArray.toArray
          })
        });
        kanonymize(leftRDDWithRange, blockedIndices1, metadata,k);
        kanonymize(rightRDDWithRange, blockedIndices2,metadata, k);
      } else {
        println("-------------->")
        linesRDD.take(1)(0)._2.foreach({ case (x, y) => println(x + "---" + y) });
      }
    }
  }

  class Category(value: String) {
    def value(): String =
      {
        return value;
      }

    var children: List[Category] = List();
    var childrenString: List[String] = List();
    var parent: Category;
    def setChildren(childrenCategory: List[Category]) {
      childrenCategory.foreach { x => this.children :+ x; this.childrenString :+ x.value() };
    }
    def setParent(parent: Category) {
      this.parent = parent;
    }
    def getParent(): Category =
      {
        return parent;
      }
  }

  class Column(name: String, index: Int, colType: Char, isQuasiIdentifier: Boolean, rootCategory: Category) {
    def getName(): String = {
      return name;
    }
    def getIndex(): Int = {
      return index;
    }
    def getColType(): Char = {
      return colType;
    }
    def getIsQuasiIdentifier(): Boolean = {
      return isQuasiIdentifier;
    }
    def getRootCategory(): Category = {
      return rootCategory;
    }

    /**
     * Given list of string values, this method finds the bottom most category that contains all elements containing given set.
     */
    def findCategory(columnValues: List[String]): Category = {
      var category = rootCategory;
      var childFound = false;

      while (category != null) {

        if (category.children != null && category.children.length > 0) {
          category.children.foreach { x =>
            {
              if (x.childrenString.intersect(columnValues).length == columnValues.length) {
                category = x;
              }
            }

          };
        } else {
          return category;
        }
      }
      return rootCategory;
    }

  }
  class Metadata(columnMetadata: Map[Int, Column]) {

    def getMetadata(columnId: Int):Option[Column]= {
      return columnMetadata.get(columnId);
    }
  }

  def findTopMostCategory(values: List[String]): Boolean =
    {

    }

  /**
   * Inner class used for sharing output of dimension with the calling method.
   */
  class Dimensions(dimension: Int, min: Double, median: Double, max: Double, leftSet:Category,rightSet:Category) extends Serializable {
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
  def selectDimension(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int],metadata: Metadata): Dimensions = {

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
    println(indexValuePairs.take(1))

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
    println("Dim : " + dimToBeReturned)
    /**
     * Find the exact list for selected dimension, sort list of values, extract middle element
     */
    
    if(metadata.getMetadata(dimToBeReturned).get.getColType()=='s')
    {
      
    }
    else
    {
      
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
    return new Dimensions(dimToBeReturned, min, median, max,null,null);
  }
  }
}