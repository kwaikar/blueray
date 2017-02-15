package edu.utd.security.blueray;

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.InterruptibleIterator
import org.apache.spark.sql.types.StringType
import edu.utd.security.risk.LBS.Metadata
import scala.io.Source
import edu.utd.security.risk.Metadata
import edu.utd.security.risk.DataReader

/**
 * Custom Implementation of InteruptibleIterator that blocks the value passed while iterating over the array.
 */
class ColumnBlockingInterruptibleIterator[T](context: TaskContext, delegate: Iterator[T], val columnsToBeBlocked: String, val dataMetadata: Metadata)
    extends InterruptibleIterator[T](context, delegate) {

  val numColumns = columnsToBeBlocked.substring(0, columnsToBeBlocked.indexOf('[')).toInt;
  val blockCols = columnsToBeBlocked.substring(columnsToBeBlocked.indexOf('[') + 1, columnsToBeBlocked.indexOf(']')).split(",").map(_.toInt);
  val metadataPath = columnsToBeBlocked.substring(columnsToBeBlocked.indexOf(']')+1, columnsToBeBlocked.length());
  /**
   * This method verifies whether next element available through iterator is authorized or not. If authorized, it holds it in the memory for serving via next method.
   */
  override def hasNext: Boolean = {

    super.hasNext;
  }
  /**
   * Returns element from memory, if not present
   */
  override def next(): T = {

    /**
     * Consume the authorized next element by returning the same
     */
    val nextElement = super.next();
    var cnt = 0;
   
    if (nextElement != null) {
      var localNextElementStr = "";
      if (nextElement.getClass == classOf[UnsafeRow]) {

        var row: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
        var objectVal: Array[Byte] = nextElement.asInstanceOf[UnsafeRow].getBytes.asInstanceOf[Array[Byte]];
        for (c <- objectVal) {
          // cnt += 1;
          localNextElementStr += (c.toChar);
          //print("[" + cnt + "=" + c.toInt + "]")
        }
      } else {
        localNextElementStr = nextElement.toString();
      }
      var split = localNextElementStr.trim().split(",");
      if (split.length == numColumns) {
        for (i <- blockCols) {
          split(i) = getStringOfLength(i, split(i));
        }

        if (nextElement.getClass == classOf[String]) {
          return split.mkString(",").asInstanceOf[T];
        } else if (nextElement.getClass == classOf[UnsafeRow]) {

          val unsafeRow: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
          var newElement: UnsafeRow = new UnsafeRow(unsafeRow.numFields());
        println(blockCols.mkString + "============================Class Type Found UnsafeRow : " + unsafeRow.numFields() + " |" + localNextElementStr.trim() + "|")
          localNextElementStr = localNextElementStr.trim().r.replaceAllIn(localNextElementStr, split.mkString(","));
          //println("replacing ++" + localNextElementStr.trim() + "|")
          //println("Returning |" + localNextElementStr.trim() + "|")
          newElement.pointTo(localNextElementStr.map(_.toByte).toArray, unsafeRow.getBaseOffset, unsafeRow.getSizeInBytes)
          return newElement.asInstanceOf[T];
        } else {
          return nextElement;
        }
      } else {
        return nextElement;
      }
    } else {
      return nextElement;
    }
  }

  def getStringOfLength(index: Int, value: String): String = {
    if (dataMetadata == null) {
      var sb: StringBuilder = new StringBuilder();
      for (c <- 1 to value.length()) {
        sb.append("-");
      }
      sb.toString
    } else {
      return dataMetadata.getMetadata(index).get.getParentCategory(value).value();
    }
  }
}
