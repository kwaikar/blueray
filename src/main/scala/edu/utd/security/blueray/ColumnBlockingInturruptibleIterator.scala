package edu.utd.security.blueray;

import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import edu.utd.security.risk.Metadata
import java.util.concurrent.ConcurrentHashMap

/**
 * Custom Implementation of InteruptibleIterator that blocks the value passed while iterating over the array.
 */
class ColumnBlockingInterruptibleIterator[T](context: TaskContext, delegate: Iterator[T], val columnsToBeBlocked: String, val dataMetadata: Metadata)
    extends InterruptibleIterator[T](context, delegate) {

  val numColumns = columnsToBeBlocked.substring(0, columnsToBeBlocked.indexOf('[')).toInt;
  val blockCols = columnsToBeBlocked.substring(columnsToBeBlocked.indexOf('[') + 1,
    columnsToBeBlocked.indexOf(']')).split(",").map(_.toInt);
  val metadataPath = columnsToBeBlocked.substring(columnsToBeBlocked.indexOf(']') + 1,
    columnsToBeBlocked.length());
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

    val nextElement = super.next();
    if (nextElement != null) {
      var localNextElementStr = "";
      if (nextElement.getClass == classOf[UnsafeRow]) {
        var objectVal: Array[Byte] = nextElement.asInstanceOf[UnsafeRow].getBytes.asInstanceOf[Array[Byte]];
        for (c <- objectVal) {
          localNextElementStr += (c.toChar);
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
          localNextElementStr = localNextElementStr.trim().r.replaceAllIn(localNextElementStr,
            split.mkString(","));
          newElement.pointTo(localNextElementStr.map(_.toByte).toArray, unsafeRow.getBaseOffset,
            unsafeRow.getSizeInBytes)
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
  var fixedMap = new ConcurrentHashMap[Int, String]()
  def getStringOfLength(index: Int, value: String): String = {
    if (dataMetadata == null) {
      var string = fixedMap.get(index);
      if (string == null) {
        var sb: StringBuilder = new StringBuilder();
        for (c <- 1 to value.length()) {
          sb.append("-");
        }
        string = sb.toString;
        fixedMap.put(index, string);
      }
      return string;
    } else {
      return dataMetadata.getMetadata(index).get.getParentCategory(value).value();
    }
  }
}
