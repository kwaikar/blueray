package  edu.utd.security.blueray

import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext
import org.apache.spark.TaskKilledException
import org.apache.spark.annotation.DeveloperApi
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
          println("checking" + localNextElement.toString())
        while (localNextElement.toString().contains(valueToBeBlocked) && hasNextVal) {

          println("Blocking" + localNextElement)
          localNextElement = super.next();
          hasNextVal = super.hasNext
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
      this.hasNext;
    }
    /**
     * Consume the authorized next element by returning the same
     */
          println("returning" + nextElement)
    nextConsumed = true;
    return nextElement;
  }

}
