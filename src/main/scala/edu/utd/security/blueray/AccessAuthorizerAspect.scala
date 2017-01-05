package edu.utd.security.blueray

import java.util.Enumeration

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Aspect implementing authorized computation of the RDD
 */
@Aspect
class AccessAuthorizerAspect {
  

  @Around(value = "execution(* org.apache.spark.rdd.MapPartitionsRDD.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")
  def aroundAdvice_spark(jp: ProceedingJoinPoint, theSplit: Partition, context: TaskContext): AnyRef = {

  //  println("----------------------- Going through the Aspect ---------------------------------");
  
    val iterator = (jp.proceed(jp.getArgs()));
    

    // if (context.getLocalProperty("PRIVILEDGE") != null) {
    val policy = getPolicy(context, jp, PointCutType.SPARK);

    if (policy != None) 
    {
      val authorizedIterator = new AuthorizedInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], policy.get.filterExpression);
      return authorizedIterator
    }
    return iterator
  }

 /* @Around(value = "execution(* org.apache.spark.sql.execution.datasources.FileScanRDD.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")
  def aroundAdvice_sparkSQL(jp: ProceedingJoinPoint, theSplit: Partition,   context: TaskContext): AnyRef = {

    val iterator = (jp.proceed(jp.getArgs()));
    if (context.getLocalProperty("PRIVILEDGE") != null) {
      val policy = getPolicy(context, jp, PointCutType.SPARKSQL);
      //println("Executing FileScanRDD iterator")
      if (policy != None) {
        val authorizedIterator = new AuthorizedInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], policy.get.filterExpression);
        return authorizedIterator
      }
    }
    return iterator;
  }*/
  def getPolicy(context: org.apache.spark.TaskContext, jp: org.aspectj.lang.ProceedingJoinPoint, pcType: Any): Option[Policy] = {
    var policy: Option[Policy] = None;
    val auth: Option[String] =Some(context.getLocalProperty("USER"))// Util.extractAuth(context)

    var path = Util.extractPathForSpark(jp);
    if (path == null || path.trim().length() == 0) {
      path = Util.extractPathForSparkSQL(jp);
    }
    policy = AccessMonitor.getPolicy(path, auth)
    policy
  }
}