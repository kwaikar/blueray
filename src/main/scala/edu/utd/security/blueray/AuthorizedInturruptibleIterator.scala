
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

  private val BLOCKED_VALUE_WRAPPER = "-";
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
      if (localNextElementStr.contains(valueToBeBlocked.trim())  ) {
        println("Blocking: " + valueToBeBlocked + " ==> " + localNextElementStr.toString().trim())
        //println("|||" + nextElement.getClass() + "====")

        if (nextElement.getClass == classOf[String]) {
          if (valueToBeBlocked.trim().length() == 0) {
            return BLOCKED_VALUE_WRAPPER.asInstanceOf[T]
          } else {
            val newElement = nextElement.toString().replaceAll(valueToBeBlocked, BLOCKED_VALUE_WRAPPER).asInstanceOf[T];
            //   println("returning :"+newElement)
            return newElement;

          }
        } else if (nextElement.getClass == classOf[UnsafeRow]) {
          val unsafeRow: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
          var newElement: UnsafeRow = new UnsafeRow(unsafeRow.numFields());
          if (valueToBeBlocked.trim().length() == 0) {
           // localNextElementStr = localNextElementStr.replaceAll(localNextElementStr.trim(), BLOCKED_VALUE_WRAPPER);
             var sb:StringBuilder = new StringBuilder();
            for(c<-localNextElementStr)
            {
              if(c.toInt>0){
              sb.append(BLOCKED_VALUE_WRAPPER);
            }
            }
            newElement.pointTo(sb.toString().getBytes, unsafeRow.getBaseOffset, sb.toString().getBytes.length)
          } else {
            var sb:StringBuilder = new StringBuilder();
            for(c<-valueToBeBlocked)
            {
              sb.append(BLOCKED_VALUE_WRAPPER);
            }
            localNextElementStr = localNextElementStr.replaceAll(valueToBeBlocked,sb.toString() );
            newElement.pointTo(localNextElementStr.getBytes, unsafeRow.getBaseOffset, unsafeRow.getSizeInBytes)
          }
          return newElement.asInstanceOf[T];
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
