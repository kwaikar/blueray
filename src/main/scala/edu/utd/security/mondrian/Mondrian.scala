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
    kanonymize(4);
  }
  var numColumns = 0;
  var metadata = new Metadata(Map());
  var rdds: List[RDD[(Long, Array[(String, Int)])]] = List();
  var summaryStatistics : List[] = List();
  def kanonymize(k: Int) {

    val file = sc.textFile("hdfs://localhost/user/adult.data2.txt")
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
    metadata = readMetadata("/home/kanchan/metadata.xml");

    sc.broadcast(metadata);
    numColumns = metadata.numColumns();

    var blockedIndices: Set[Int] = Set(); ;
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
    writeOutputToFile(rdds, "/home/kanchan/op.txt");
  }
  def writeOutputToFile(rdds: List[RDD[(Long, Array[(String, Int)])]], filePath: String) {
    val rddsMerged = sc.union(rdds).sortBy(_._1);
    val rddString = rddsMerged.map({
      case (rowIndex, data) => {
        var sb: StringBuilder = new StringBuilder();
        var map: Map[Int, String] = Map[Int, String]();
        data.foreach(columnIndexValuePair => {
          map += ((columnIndexValuePair._2, columnIndexValuePair._1));
        })
        for (i <- 0 to metadata.numColumns() - 1) {
          if (map.get((metadata.numColumns() + i)) == None) {
            if (metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
              sb.append("*");
            } else {
              sb.append(map.get(i).get)
            }
          } else {
            sb.append(map.get((metadata.numColumns() + i)).get);
          }
          if (i != data.length - 1) {
            sb.append(",");
          }
        }
        sb.toString();
      }
    });
    rddString.coalesce(1, true).saveAsTextFile(filePath)
  }
  def readMetadata(filePath: String): Metadata =
    {
      var columns: Map[Int, Column] = Map[Int, Column]();
      val xml = XML.loadFile("/home/kanchan/metadata.xml");
      val iterator = xml.\\("columns").\("column").iterator;
      while (iterator.hasNext) {
        val node = iterator.next;
        if (node.\("hierarchy").text.length() > 0) {
          val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, getHierarchy(node.\("hierarchy"), "*"));

          columns += ((column.getIndex(), column));
        } else {
          val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, null);

          columns += ((column.getIndex(), column));
        }
      }
      return new Metadata(columns);
    }

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
  def kanonymize(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int], metadata: Metadata, k: Int) {
    val count = linesRDD.count();
    println("First call" + count);
    if (count < k) {
      println("Invalid cut : Cluster is already smaller than value of K")
      System.exit(0);
    } else if (count == k) {
      assignSummaryStatisticAndAddToList(linesRDD);
      println("Cannot perform cut. Cluster is exactly of size of K");
    } else {
      val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices, metadata, k);
      sc.broadcast(dimAndMedian);
      println(" =>" + dimAndMedian.dimension() + " (" + dimAndMedian.min() + "-" + dimAndMedian.median() + "-" + dimAndMedian.max());
      blockedIndices.+(dimAndMedian.dimension());
      var blockedIndices1: Set[Int] = Set();
      blockedIndices1 ++= blockedIndices;
      var blockedIndices2: Set[Int] = Set();
      blockedIndices2 ++= blockedIndices;

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
  def assignSummaryStatisticAndAddToList(linesRDD: RDD[(Long, Array[(String, Int)])]) {

    val indexValuePairs = linesRDD.flatMap({ case (index1, list) => (list) }).map({
      case (value, index) => (index, value.trim())
    });
    val indexValueGrouped = indexValuePairs.groupByKey().map({ case (index, list) => (index, list.toList.distinct) }).filter(_._1 < metadata.numColumns()).map({
      case (x, y) =>
        if (metadata.getMetadata(x).get.getColType() == 's') {
          (x, y.mkString(","));
        } else {
          val listOfNumbers = y.map(_.toDouble);
          (x, listOfNumbers.min + "-" + listOfNumbers.max);
        }
    });
    println("-------------------");

    var map: Map[Int, String] = Map[Int, String]();
    indexValueGrouped.collect().foreach({ case (x, y) => map += ((x, y)) });
    println("map" + map);
    println("-------------------");
    var category = ("");
    var updatedRDD = linesRDD;
    for (index <- 0 to metadata.numColumns - 1) {
      val column = metadata.getMetadata(index).get;
      if (column.getIsQuasiIdentifier()) {
        if (column.getColType() == 's') {
          if (column.getRootCategory() == null) {
            category = "*";
          } else {
            category = column.findCategory(map.get(column.getIndex()).get.split(",")).value();
          }
        } else {
          category = map.get(column.getIndex()).get;
        }
 
        println(category + " " + index);
        updatedRDD = updatedRDD.map({
          case (x, y) =>
          println(y.mkString(",")+":"+index+" : "+category);
            var newArray: ListBuffer[(String, Int)] = new ListBuffer();
            val marker = y.filter(_._2 == index).take(1)(0)._2;
            newArray ++= y.slice(0, marker );
            newArray += ((category, index))
            newArray ++= y.slice(marker + 1, y.length - 1);
            (x, newArray.toArray)
        });

        println(category + " " + index);
        updatedRDD.collect().foreach({ case (x, y) => println(x + " : " + y.mkString(",")) });
      }
    }

    rdds = rdds :+ updatedRDD;
    printRDD(updatedRDD)
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

  def printRDD(linesRDD: org.apache.spark.rdd.RDD[(Long, Array[(String, Int)])]) = {
    linesRDD.take(1).foreach(println);
  }

  /**
   * Class responsible for holding hierarchy for categorical values.
   */
  class Category(value: String) extends Serializable {
    def value(): String =
      {
        return value;
      }
    var children: List[Category] = List();
    var childrenString: List[String] = List(value);
    def addChildren(childrenCategory: Category) {
      this.children = this.children :+ childrenCategory;
      this.childrenString = this.childrenString :+ childrenCategory.value();
      this.childrenString ++= childrenCategory.childrenString;
    }
    override def toString: String = {
      return value + "(" + value + "=" + childrenString.mkString + ")=>" + "[" + children.foreach { x => x.toString() } + "]";
    }
  }

  /**
   * Class responsible for holding details of column object.
   */
  class Column(name: String, index: Int, colType: Char, isQuasiIdentifier: Boolean, rootCategory: Category) extends Serializable {
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
    override def toString: String = {
      if (rootCategory == null)
        return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + "]";
      else
        return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + rootCategory.toString + "]";
    }
    /**
     * Given list of string values, this method finds the bottom most category that contains all elements containing given set.
     */
    def findCategory(columnValues: Array[String]): Category = {

      var category = rootCategory;
      var childFound = true;

      while (childFound) {

        if (category.children != null && category.children.size > 0) {
          childFound = false;
          val childrens = category.children.toArray
          for (i <- 0 to childrens.size - 1) {
            if (childrens(i).childrenString.intersect(columnValues).length == columnValues.length) {
              category = childrens(i);
              childFound = true;
            }
          }
          if (!childFound) {
            return category;
          }

        } else {
          return category;
        }
      }
      return rootCategory;
    }
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
   * class used for sharing output of dimension with the calling method.
   */
  class Dimensions(dimensionValue: Int, minValue: Double, medianValue: Double, maxValue: Double, leftArray: Array[String], rightArray: Array[String]) extends Serializable {
    def dimension(): Int =
      {
        dimensionValue
      }
    def min(): Double =
      {
        minValue
      }
    def median(): Double =
      {
        this.medianValue
      }
    def max(): Double =
      {
        maxValue
      }

    def leftSet(): Array[String] =
      {
        leftArray
      }

    def rightSet(): Array[String] =
      {
        rightArray
      }
  }
  /**
   * Accept RDD containing row numbers and column values along with their index.
   */
  def selectDimension(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int], metadata: Metadata, k: Int): Dimensions = {

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