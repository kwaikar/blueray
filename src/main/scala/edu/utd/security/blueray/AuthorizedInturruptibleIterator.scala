package edu.utd.security.blueray

import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * Custom Implementation of InteruptibleIterator that blocks the value passed while iterating over the array.
 */
class AuthorizedInterruptibleIterator[T](context: TaskContext, delegate: Iterator[T], valueToBeBlocked: String)
    extends InterruptibleIterator[T](context, delegate) {

  private var nextElement: T = _;
  private var nextConsumed = true;

  /**
   * This method verifies whether next element available through iterator is authorized or not. If authorized, it holds it in the memory for serving via next method.
   */
  override def hasNext: Boolean = {
    if (!nextConsumed) {
      return true;
    } else {
      var hasNextVal = super.hasNext;
      if (hasNextVal) {

        var localNextElement = super.next()
        hasNextVal = super.hasNext;
        // SparkSQL specific
        var localNextElementStr = "";
        /* if (localNextElement.getClass == classOf[UnsafeRow]) {

          var objectVal: Array[Byte] = localNextElement.asInstanceOf[UnsafeRow].getBytes.asInstanceOf[Array[Byte]];
          for (c <- objectVal) {
            localNextElementStr += c.toChar
          }

          println(":checking " + localNextElementStr.trim())
          while (localNextElementStr.contains(valueToBeBlocked) && hasNextVal) {

            println("Blocking" + localNextElementStr)
            localNextElement = super.next();
            localNextElementStr = new String(localNextElement.asInstanceOf[UnsafeRow].getBytes);
            hasNextVal = super.hasNext
          }
        }
         else */ {

          println("checking " + localNextElement + " : " + localNextElement.getClass.getSimpleName)
          while (localNextElement.toString().contains(valueToBeBlocked) && hasNextVal) {

            println("Blocking" + localNextElement)
            localNextElement = super.next();
            hasNextVal = super.hasNext
          }
          // localNextElementStr = localNextElement.toString()
        }
        /**
         * Iterator could
         */
        if (localNextElement != null && (!localNextElement.toString().contains(valueToBeBlocked))) {
          nextElement = localNextElement;
          /**
           * Enable element for consumption
           */
          nextConsumed = false;
          println("setting for consumption" + nextElement)

          return true
        }
      }
      return false;
    }
  }

  /**
   * Returns element from memory, if not present
   */
  override def next(): T = {
    if (nextConsumed) {
      println("nothing to consume")
      this.hasNext;
    }
    /**
     * Consume the authorized next element by returning the same
     */
    nextConsumed = true;
    return nextElement;
  }

}
