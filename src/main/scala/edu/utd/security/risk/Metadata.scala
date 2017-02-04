package edu.utd.security.risk

import scala.collection.mutable.ListBuffer

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
  var columns: Array[Column] = null;
  
  def getQuasiColumns(): Array[Column] = {
    if (columns == null) {
      var localColumns = ListBuffer[Column]();
      for (i <- 0 to numColumns() - 1) {
        val column = getMetadata(i).get;
        if (column.getIsQuasiIdentifier()) {
          localColumns += column;
        }
      }
      columns  = localColumns.toArray;
    }
    return columns
  }
  override def toString: String = {
    return columnMetadata.mkString;
  }
}