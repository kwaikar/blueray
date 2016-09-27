package edu.utd.security.blueray

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect

/**
 * Aspect implementing authorized computation of the RDD
 */
@Aspect
class AccessAuthorizerAspect {

  // val logger = Logger(LoggerFactory.getLogger(this.getClass))
  /* 
    @Around(value = "execution(* org.apache.spark.streaming.dstream.MappedDStream.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")*/

  @Around(value = "execution(* org.apache.spark.rdd.MapPartitionsRDD.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")
  def aroundAdvice_SparkStreaming(jp: ProceedingJoinPoint, theSplit: Partition, job: JobConf, context: TaskContext): AnyRef = {
    // logger.debug("Invoking advice")
    var path: String = "";
    var auth = context.getLocalProperty("PRIVILEDGE")

    if (auth != null) {
      var pathFound = false;
      breakable {
        for (argument <- jp.getArgs()) {
          for (field <- argument.getClass.getDeclaredFields) {
            if (field.getName.equalsIgnoreCase("inputSplit") || field.getName.equalsIgnoreCase("split")) {
              field.setAccessible(true)
              val fullPath = field.get(jp.getArgs()(0)).toString()
              path = fullPath.subSequence(0, fullPath.lastIndexOf(":")).toString()
              pathFound = true;
              break;
            } else if (field.getName.equalsIgnoreCase("files")) {
              field.setAccessible(true)
              val partitionedFile = field.get(jp.getArgs()(0)).toString()
              println(partitionedFile.toString())
              path = partitionedFile.subSequence(partitionedFile.indexOf(" "), partitionedFile.indexOf(",")).toString();
              pathFound = true;
              break;
            }
          }
          if (pathFound) {
            break;
          }
        }
      }
      val iterator = (jp.proceed(jp.getArgs()))
      val policyFound = AccessMonitor.getPolicy(path, auth)
      if (policyFound != None) {
        val authorizedIterator = new AuthorizedInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], "Lii");
        return authorizedIterator
      }
      return iterator;
    } else {

      return (jp.proceed(jp.getArgs()))
    }
  }
}