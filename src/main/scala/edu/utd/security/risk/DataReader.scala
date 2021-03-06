package edu.utd.security.risk

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.xml.NodeSeq
import scala.xml.XML

/**
 * This class is responsible for reading
 */
class DataReader() extends Serializable {

  /**
   * This method reads input Data file and returns the linesRDD.
   */
  def readDataFile(sc: SparkContext,hdfsDataPath: String, numPartitions:Int): RDD[(Long, Map[Int, String])] = {
    val file = sc.textFile(hdfsDataPath ,numPartitions)
    /**
     * Split by new line, filter lines containing missing data.
     */
    val lines = file.flatMap(_.split("\n"))

    var linesWithIndex=lines.zipWithIndex();
 
    /**
     * split columns inside each line, zip with index.
     */
    val linesRDD = linesWithIndex.map({ case (value, index) => (index, value.split(",").zipWithIndex) }).map({
      case (index, data) =>
        var map: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]();
        data.foreach(columnIndexValuePair => {
          map += ((columnIndexValuePair._2, columnIndexValuePair._1.trim()));
        })
        (index, map.toMap);
    })
    return linesRDD;
  }

  def readDataFileToNativeFormat(sc: SparkContext,hdfsDataPath: String, numPartitions:Int): RDD[(Long, (String,Int,Int,String))] = {
    val file = sc.textFile(hdfsDataPath ,numPartitions)
    /**
     * Split by new line, filter lines containing missing data.
     */
    val lines = file.flatMap(_.split("\n"))

    var linesWithIndex=lines.zipWithIndex();
    /**
     * Retain indices of lines
     */
    if (true) {
      linesWithIndex = lines.filter { !_.contains("?") }.zipWithIndex;
    }  

    /**
     * split columns inside each line, zip with index.
     */
    val linesRDD = linesWithIndex.map({ case (value, index) => (index, value.split(",")) }).map({
      case (index, data) =>
    
        (index, (data(0),data(1).toDouble.intValue(),data(2).toDouble.intValue(),data(3)));
    })
    return linesRDD;
  }

  
  /**
   * This method reads metadata object from an xml file.
   */
  def readMetadata(data: String): Metadata =
    {
      var columns: Map[Int, Column] = Map[Int, Column]();
      val xml = XML.loadString(data);
      val iterator = xml.\\("columns").\("column").iterator;
      while (iterator.hasNext) {
        val node = iterator.next;
        if (node.\("type").text.charAt(0) == 's') {
          /**
           * For String column types.
           */
          if (node.\("hierarchy").text.length() > 0) {
            val column = new Column(node.\("name").text, node.\("index").text.toInt,true, node.\("isQuasiIdentifier").text.toBoolean, getHierarchy(node.\("hierarchy"), "*",null),-1,-1, node.\("num_unique").text.toInt);
            columns += ((column.getIndex(), column));
          } else {
            val column = new Column(node.\("name").text, node.\("index").text.toInt, true, node.\("isQuasiIdentifier").text.toBoolean, new Category("*",null),-1,-1, node.\("num_unique").text.toInt);
            columns += ((column.getIndex(), column));
          }
        } else {
          /**
           * Numeric columns.
           */
          val column = new Column(node.\("name").text, node.\("index").text.toInt, false, node.\("isQuasiIdentifier").text.toBoolean,getHierarchy(node.\("hierarchy"), node.\("min").text.toDouble+"_"+node.\("max").text.toDouble,null),node.\("min").text.toDouble,node.\("max").text.toDouble, node.\("num_unique").text.toInt);
          columns += ((column.getIndex(), column));
        }
      }
      return new Metadata(columns);
    }

  /**
   * This method accepts a categorical node sequence and parses entire generalization hierarchy and returns the root.
   */
  def getHierarchy(node: NodeSeq, name: String, parent:Category): Category = {

    var category = new Category(name,parent);
    node.\("children").foreach { x =>
      {
        category.addChildren(getHierarchy(x, x.\("value").text,category));
      }
    };
    return category;
  }
}