package edu.utd.security.mondrian

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.xml.XML
import scala.xml.NodeSeq
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.xml.Node

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
 val metadata: Metadata = readMetadata("/home/kanchan/metadata.xml");
    numColumns = metadata.numColumns();
    
    var blockedIndices: Set[Int] = Set( 0,11,12,10,2, 3,  6, 7, 8, 9, 14, 13);
    for (i <- 0 to numColumns - 1) {
      blockedIndices += (numColumns + i)
    }
    println(blockedIndices.mkString(","));
   
    for (i <- 0 to numColumns-1) {
      if (!metadata.getMetadata(i).get.getIsQuasiIdentifier()) {
        println("Blocking index as it is not a Quasi-Identifier");
        blockedIndices += i;
      }
    }
    sc.broadcast(numColumns);
    val k = 4;
    kanonymize(linesRDD, blockedIndices, metadata, k)
  }
  def readMetadata(filePath: String): Metadata =
    {
      var columns: Map[Int, Column] = Map[Int, Column]();
      val xml = XML.loadFile("/home/kanchan/metadata.xml");
      val iterator = xml.\\("columns").\("column").iterator;
      while (iterator.hasNext) {
        val node = iterator.next;
        if (node.\("hierarchy").text.length() > 0) {
          val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, getHierarchy(node.\("hierarchy"),"root"));

          println(column.toString);
          columns += ((column.getIndex(), column));
        } else {
          val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, null);

          println(column.toString);
          columns += ((column.getIndex(), column));
        }
      }
      return new Metadata(columns);
    }

  def getHierarchy(node: NodeSeq,name:String): Category = {

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
    } else if (count == k) {
      println("-------------->")

      linesRDD.take(1)(0)._2.foreach({ case (x, y) => println(x + "---" + y) });
      println("Cannot perform cut. Cluster is exactly of size of K");
    } else {
      val dimAndMedian: Dimensions = selectDimension(linesRDD, blockedIndices, metadata);
      sc.broadcast(dimAndMedian.dimension());
      sc.broadcast(dimAndMedian.median());
      sc.broadcast(dimAndMedian.min());
      sc.broadcast(dimAndMedian.max());
      sc.broadcast(dimAndMedian.leftSet());
      sc.broadcast(dimAndMedian.rightSet());
      println(" =>" + dimAndMedian.dimension() + " (" + dimAndMedian.min() + "-" + dimAndMedian.median() + "-" + dimAndMedian.max());
      blockedIndices.+(dimAndMedian.dimension());
      var blockedIndices1: Set[Int] = Set();
      blockedIndices1 ++= blockedIndices;
      var blockedIndices2: Set[Int] = Set();
      blockedIndices2 ++= blockedIndices;

      val sortedRDD = linesRDD.sortBy({ case (x, y) => y(dimAndMedian.dimension())._1 }, true);

      if (metadata.getMetadata(dimAndMedian.dimension()).get.getColType() == 's') {
        println(dimAndMedian.leftSet().mkString+" -- "+dimAndMedian.rightSet().mkString);
        val leftRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.leftSet().contains(y(dimAndMedian.dimension())._1.trim()) } });
        val rightRDD = linesRDD.filter({ case (x, y) => { dimAndMedian.rightSet().contains(y(dimAndMedian.dimension())._1.trim()) } });
        val leftSize = leftRDD.count();
        val rightSize = rightRDD.count();

        println(leftSize+ ":"+rightSize); 
        if (leftSize >= k && rightSize >= k) {

          val leftCategory: Category = metadata.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.leftSet())
          val rightCategory: Category = metadata.getMetadata(dimAndMedian.dimension()).get.findCategory(dimAndMedian.rightSet())

          val leftRDDWithRange = getStringRDDWithRange(leftRDD, dimAndMedian.dimension(), leftCategory.value().trim());
          val rightRDDWithRange = getStringRDDWithRange(rightRDD, dimAndMedian.dimension(), rightCategory.value().trim());
          /**
           * Add the range value applicable to all left set elements
           */
          println("Making the cut on " + dimAndMedian.dimension()+ " "+leftCategory.value().trim()+" : "+rightCategory.value().trim());

          kanonymize(leftRDDWithRange, blockedIndices1, metadata, k);
          kanonymize(rightRDDWithRange, blockedIndices2, metadata, k);
        } else {
          printRDD(linesRDD)
        }
      } else {
        println(dimAndMedian.dimension()+"-->"+metadata.getMetadata(dimAndMedian.dimension()).toString());
        val leftRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble <= dimAndMedian.median().toDouble });
        val rightRDD = linesRDD.filter({ case (x, y) => y(dimAndMedian.dimension())._1.trim().toDouble > dimAndMedian.median().toDouble });
        val leftSize = leftRDD.count();
        val rightSize = rightRDD.count();
        if (leftSize >= k && rightSize >= k) {

          /**
           * Add the range value applicable to all left set elements
           */
          println("Making the cut on " + dimAndMedian.dimension());

          val leftRDDWithRange = getNumericRDDWithRange(leftRDD, dimAndMedian.dimension(), dimAndMedian.min() + "_" + dimAndMedian.median());
          val rightRDDWithRange = getNumericRDDWithRange(rightRDD, dimAndMedian.dimension(), dimAndMedian.median() + "_" + dimAndMedian.max());
          kanonymize(leftRDDWithRange, blockedIndices1, metadata, k);
          kanonymize(rightRDDWithRange, blockedIndices2, metadata, k);
        } else {
          printRDD(linesRDD)
        }
      }
    }

  }

  def getStringRDDWithRange(rightRDD: RDD[(Long, Array[(String, Int)])], dimension: Int, newValue: String): RDD[(Long, Array[(String, Int)])] =
    {
      val rightRDDWithRange = rightRDD.map({
        case (value, linesArray) => (value, {
          var index = -1;
          for (i <- 0 to linesArray.length - 1) {
            if (linesArray(i)._2 == (numColumns + dimension)) {
              index = i;
            }
          }
          var newArray: ListBuffer[(String, Int)] = new ListBuffer();
          newArray ++= linesArray;
          if (index == -1) {
            newArray += new Tuple2(newValue, (numColumns + dimension));
          } else {
            newArray.update(index, new Tuple2(newValue, (numColumns + dimension)));
          }
          newArray.toArray
        })
      })
      return rightRDDWithRange;
    }

  def getNumericRDDWithRange(rightRDD: RDD[(Long, Array[(String, Int)])], dimension: Int, newValue: String): RDD[(Long, Array[(String, Int)])] =
    {
      val rightRDDWithRange = rightRDD.map({
        case (value, linesArray) => (value, {
          var index = -1;
          for (i <- 0 to linesArray.length - 1) {
            if (linesArray(i)._2 == (numColumns + dimension)) {
              index = i;
            }
          }
          var newArray: ListBuffer[(String, Int)] = new ListBuffer();
          newArray ++= linesArray;
          if (index == -1) {
            newArray += new Tuple2(newValue, (numColumns + dimension));
          } else {
            newArray.update(index, new Tuple2(newValue, (numColumns + dimension)));
          }
          newArray.toArray
        })
      })
      return rightRDDWithRange;
    }

  def printRDD(linesRDD: org.apache.spark.rdd.RDD[(Long, Array[(String, Int)])]) = {
    println("-------------->")
    linesRDD.take(1)(0)._2.foreach({ case (x, y) => println(x + "---" + y) });
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
      this.children=this.children :+ childrenCategory;
      this.childrenString=this.childrenString :+ childrenCategory.value();
      this.childrenString++=childrenCategory.childrenString;
      println("Added "+this.childrenString)
    }
    override def toString: String = {
      return value + "(" +value+"="+ childrenString.mkString + ")=>" + "[" + children.foreach { x => x.toString() } + "]";
    }
  }

  /**
   * Class responsible for holding details of column object.
   */
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
    override def toString: String = {
      if(rootCategory==null)
      return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + "]";
      else
        return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + rootCategory.toString+"]";
    }
    /**
     * Given list of string values, this method finds the bottom most category that contains all elements containing given set.
     */
    def findCategory(columnValues: Array[String]): Category = {
      var category = rootCategory;
      var childFound = true;

      while (childFound) {

        if (category.children != null && category.children.size > 0) {
          childFound=false;
          val childrens=category.children.toArray
            println("-----------"+category.children.length+" "+category.children.mkString(" |_| "));
          for(i<-0 to childrens.size-1)
          {
            println(childrens(i));
            if(childrens(i).childrenString.intersect(columnValues).length == columnValues.length)
            {
                category = childrens(i);
                childFound=true;
            }
          }
          if(!childFound)
          {
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
  def selectDimension(linesRDD: RDD[(Long, Array[(String, Int)])], blockedIndices: Set[Int], metadata: Metadata): Dimensions = {

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

    if (metadata.getMetadata(dimToBeReturned).get.getColType() == 's') {
      // distribute keys by putting alternate ones in alternate list. This way two partition sizes should roughly be near each other

      val sortedListOfValues = indexValueGrouped.filter(_._1 == dimToBeReturned).flatMap({ case (x, y) => (y) }).map(x => (x.trim(), 1)).reduceByKey((a, b) => a + b).sortByKey(false).keys.zipWithIndex;
      var leftList = sortedListOfValues.filter { case (x, y) => y % 2 == 0 }.keys.collect();
      var rightList = sortedListOfValues.filter { case (x, y) => y % 2 == 1 }.keys.collect();
      return new Dimensions(dimToBeReturned, 0, 0, 0, leftList, rightList);

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