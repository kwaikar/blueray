
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

  val BLOCKED_VALUE_WRAPPER = "-";
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
var cnt=0;
    if (nextElement != null) {
      var localNextElementStr = "";
      if (nextElement.getClass == classOf[UnsafeRow]) {

        var row: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
        var objectVal: Array[Byte] = nextElement.asInstanceOf[UnsafeRow].getBytes.asInstanceOf[Array[Byte]];
        var sb: StringBuilder = new StringBuilder();
        for (c <- objectVal) {
          cnt+=1;
            sb.append(c.toChar);
            print("["+cnt+"="+c.toInt+"]")
        }
        localNextElementStr = sb.toString();

      } else {
        localNextElementStr = nextElement.toString();
      }
      println("Checking:"+localNextElementStr.trim()+" : " +(valueToBeBlocked.r.findAllIn(localNextElementStr).length>0));
      if (localNextElementStr.trim().length() > 0 && (valueToBeBlocked.r.findAllIn(localNextElementStr).length>0)) {
        println("Blocking: " + valueToBeBlocked + " ==> " + localNextElementStr.toString().trim())

        if (nextElement.getClass == classOf[String]) {
          if (valueToBeBlocked.trim().length() == 0) {
            return BLOCKED_VALUE_WRAPPER.asInstanceOf[T]
          } else {
            var replacedString: String = Util.getStringOfLength(valueToBeBlocked.toCharArray().length);
            return nextElement.toString().replaceAll(valueToBeBlocked, replacedString).asInstanceOf[T];
          }
        } else if (nextElement.getClass == classOf[UnsafeRow]) {
          val unsafeRow: UnsafeRow = nextElement.asInstanceOf[UnsafeRow];
          var newElement: UnsafeRow = new UnsafeRow(unsafeRow.numFields());
          if (valueToBeBlocked.trim().length() == 0) {
            var sb: StringBuilder = new StringBuilder();
            for (c <- localNextElementStr) {
              if (c.toInt > 0) {
                sb.append(BLOCKED_VALUE_WRAPPER);
              }
            }
            println("========pointing===========>>")
            newElement.pointTo(unsafeRow.getBytes, unsafeRow.getBaseOffset,  unsafeRow.getBytes.length)
          } else {
 
            
    if (valueToBeBlocked.r.findFirstIn(localNextElementStr) != None && valueToBeBlocked.r.findFirstIn(localNextElementStr).get.length() > 0) {
     
            var replaceMent: String = Util.getStringOfLength(valueToBeBlocked.r.findFirstIn(localNextElementStr).get.length());
            localNextElementStr = valueToBeBlocked.r.replaceAllIn(localNextElementStr, replaceMent );

            println("===================>>"+localNextElementStr.trim()+":"+localNextElementStr.getBytes.length+" : "+cnt+":"+unsafeRow.getBaseOffset+"=>"+unsafeRow.getSizeInBytes)   
            
            newElement.pointTo(localNextElementStr.map(_.toByte).toArray, unsafeRow.getBaseOffset, unsafeRow.getBytes.length) 
    }
          }
          println("-->Returned!!!:")
          var cnt2=0;
           for (c <- localNextElementStr.map(_.toByte).toArray) {
          cnt2+=1;
            print("["+cnt2+"="+c.toInt+"]")
        }
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
