package edu.utd.security.risk

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.xml.NodeSeq
import scala.xml.XML



/**
 * This class is responsible for reading
 */
class DataReader(sc: SparkContext) extends Serializable {

  /**
   * This method reads input Data file and returns the linesRDD.
   */
  def readDataFile(hdfsDataPath: String, skipMissingRecords: Boolean): RDD[(Long, scala.collection.mutable.Map[Int, String])] = {
    val file = sc.textFile(hdfsDataPath, 8)
    /**
     * Split by new line, filter lines containing missing data.
     */
    val lines = file.flatMap(_.split("\n"))

    var linesWithIndex=lines.zipWithIndex();
    /**
     * Retain indices of lines
     */
    if (skipMissingRecords) {
      linesWithIndex = lines.filter { !_.contains("?") }.zipWithIndex;
    }  

    /**
     * split columns inside each line, zip with index.
     */
    val linesRDD = linesWithIndex.map({ case (value, index) => (index, value.split(",").zipWithIndex) }).map({
      case (index, data) =>
        var map: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]();
        data.foreach(columnIndexValuePair => {
          map += ((columnIndexValuePair._2, columnIndexValuePair._1.trim()));
        })
        (index, map);
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
            val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, getHierarchy(node.\("hierarchy"), "*"),-1,-1, node.\("num_unique").text.toInt);
            columns += ((column.getIndex(), column));
          } else {
            val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean, new Category("*"),-1,-1, node.\("num_unique").text.toInt);
            columns += ((column.getIndex(), column));
          }
        } else {
          /**
           * Numeric columns.
           */
          val column = new Column(node.\("name").text, node.\("index").text.toInt, node.\("type").text.charAt(0), node.\("isQuasiIdentifier").text.toBoolean,getHierarchy(node.\("hierarchy"), node.\("min").text.toDouble+"_"+node.\("max").text.toDouble),node.\("min").text.toDouble,node.\("max").text.toDouble, node.\("num_unique").text.toInt);
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
}