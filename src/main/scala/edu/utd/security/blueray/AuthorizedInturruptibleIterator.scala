
package edu.utd.security.blueray

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.InterruptibleIterator
import org.apache.spark.sql.types.StringType

/**
 * Custom Implementation of InteruptibleIterator that blocks the value passed while iterating over the array.
 */
class AuthorizedInterruptibleIterator[T](context: TaskContext, delegate: Iterator[T], val valueToBeBlocked: String)
    extends InterruptibleIterator[T](context, delegate) {

  private val BLOCKED_VALUE_WRAPPER = "_BLOCKED_";
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

  //   println("Called " + valueToBeBlocked.trim())
    /**
     * Consume the authorized next element by returning the same
     */
    val nextElement = super.next();

    if (nextElement != null) {
      var localNextElementStr = "";
      if (nextElement.getClass == classOf[UnsafeRow]) {

        var row: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];

        var objectVal: Array[Byte] = nextElement.asInstanceOf[UnsafeRow].getBytes.asInstanceOf[Array[Byte]];
        for (c <- objectVal) {
          localNextElementStr += c.toChar
        }
        //println("==>" + localNextElementStr)
      } else {
        localNextElementStr = nextElement.toString();
      }
      if (localNextElementStr.contains(valueToBeBlocked.trim())) {
      //  println("Blocking: " +valueToBeBlocked+" ==> "+ localNextElementStr.toString().trim())
        //println("|||" + nextElement.getClass() + "====")

        if (nextElement.getClass == classOf[String]) {
          val newElement = nextElement.toString().replaceAll(valueToBeBlocked, BLOCKED_VALUE_WRAPPER).asInstanceOf[T];
       //   println("returning :"+newElement)
          return newElement;
        } else if (nextElement.getClass == classOf[UnsafeRow]) {
          val unsafeRow: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
          var newElement: UnsafeRow = new UnsafeRow(unsafeRow.numFields());
          var value: Array[Byte] = (unsafeRow.getBaseObject).asInstanceOf[Array[Byte]]
          localNextElementStr = localNextElementStr.replaceAll(valueToBeBlocked, BLOCKED_VALUE_WRAPPER);
          //println("newValue: " + localNextElementStr.toString().trim())
          newElement.pointTo(localNextElementStr.getBytes, unsafeRow.getBaseOffset, unsafeRow.getSizeInBytes)

          return  newElement.asInstanceOf[T];
        } else {
          return nextElement;
        }
      } else {
        //println("Passing original" + nextElement)
        return nextElement;
      }
    } else {

      return nextElement;
    }
  }

}
