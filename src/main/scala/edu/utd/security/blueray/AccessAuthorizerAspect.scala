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
    @Around(value = "execution(* org.apache.spark.streaming.dstream.MappedDStream.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")
  def aroundAdvice_PartitionCompute(jp: ProceedingJoinPoint, theSplit: Partition, job: JobConf, context: TaskContext): AnyRef = {
   // logger.debug("Invoking advice")
    val args = jp.getArgs();
    var pathFound = false;
    var path: String = "";
    
    breakable {
      for (argument <- args) {
        for (field <- argument.getClass.getDeclaredFields) {
          if (field.getName.equalsIgnoreCase("inputSplit")) {
            field.setAccessible(true)
            val fullPath = field.get(args(0)).toString()
            path = fullPath.subSequence(0, fullPath.lastIndexOf(":")).toString()
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
    val policyFound = AccessAuthorizationManager.getPolicy(path)
    if (policyFound != None) {

      val authorizedIterator = new AuthorizedInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], "Lii");
      return authorizedIterator
    }
    return iterator;
    }*/

  @Around(value = "execution(* org.apache.spark.rdd.MapPartitionsRDD.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")
  def aroundAdvice_SparkStreaming(jp: ProceedingJoinPoint, theSplit: Partition, job: JobConf, context: TaskContext): AnyRef = {
    // logger.debug("Invoking advice")
    val args = jp.getArgs();
    var pathFound = false;
    var path: String = "";
    println("Excuting Aspect")
    breakable {
      for (argument <- args) {
        println("argument-->" + argument)
        for (field <- argument.getClass.getDeclaredFields) {
          if (field.getName.equalsIgnoreCase("inputSplit") || field.getName.equalsIgnoreCase("split")) {
            field.setAccessible(true)
            val fullPath = field.get(args(0)).toString()
            path = fullPath.subSequence(0, fullPath.lastIndexOf(":")).toString()
            pathFound = true;
            break;
          } else if (field.getName.equalsIgnoreCase("files")) {
            println("Wrapped array foound")
            field.setAccessible(true)

            val partitionedFile = field.get(args(0)).toString()
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
    println("path found" + path)
    val iterator = (jp.proceed(jp.getArgs()))
    val policyFound = AccessAuthorizationManager.getPolicy(path)
    if (policyFound != None) {
      println("Policy found"+policyFound)
      val authorizedIterator = new AuthorizedInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], "Lii");
      return authorizedIterator
    }
    return iterator;
  }
}