package edu.utd.security.blueray;

import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import edu.utd.security.risk.LBSAlgorithm
import edu.utd.security.risk.LBSMetadata

/**
 * Custom Implementation of InteruptibleIterator that blocks the value passed while iterating over the array.
 */
class LBSInterruptibleIterator[T](context: TaskContext, delegate: Iterator[T], algorithm:LBSAlgorithm)
    extends InterruptibleIterator[T](context, delegate) {

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
      if (localNextElementStr.split(",").length == LBSMetadata.getInstance().numColumns()) {
         
    val optimalRecord = algorithm.findOptimalStrategy(localNextElementStr)

        if (nextElement.getClass == classOf[String]) {
          return algorithm.findOptimalStrategy(localNextElementStr.trim()).asInstanceOf[T];
        } else if (nextElement.getClass == classOf[UnsafeRow]) {
          val unsafeRow: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
          var newElement: UnsafeRow = new UnsafeRow(unsafeRow.numFields());
          localNextElementStr = localNextElementStr.trim().r.replaceAllIn(localNextElementStr, algorithm.findOptimalStrategy(localNextElementStr.trim()));
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
 }
