
package edu.utd.security.blueray

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.InterruptibleIterator

/**
 * Custom Implementation of InteruptibleIterator that blocks the value passed while iterating over the array.
 */
class AuthorizedInterruptibleIterator[T](context: TaskContext, delegate: Iterator[T], valueToBeBlocked: String)
    extends InterruptibleIterator[T](context, delegate) {

  private val BLOCKED_VALUE_WRAPPER = "**********";
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
    var nextElement = super.next();

    if (nextElement != null) {
      var localNextElementStr = "";
      if (nextElement.getClass == classOf[UnsafeRow]) {

        var objectVal: Array[Byte] = nextElement.asInstanceOf[UnsafeRow].getBytes.asInstanceOf[Array[Byte]];
        for (c <- objectVal) {
          localNextElementStr += c.toChar
        }
      } else {
        localNextElementStr = nextElement.toString();
      }
      if (localNextElementStr.contains(valueToBeBlocked)) {
        println("Blocking: " + localNextElementStr)
        nextElement = Class.forName(nextElement.getClass().toString()).newInstance().asInstanceOf;
      }
    }
    return nextElement;
  }

}
