package edu.utd.security.blueray

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.InterruptibleIterator
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import scala.io.Source
import edu.utd.security.risk.DataReader
import edu.utd.security.risk.Metadata
import edu.utd.security.risk.LBSMetadata
import edu.utd.security.risk.LBSAlgorithm
import edu.utd.security.risk.LBSParameters

/**
 * Aspect implementing authorized computation of the RDD
 */
@Aspect
class AccessAuthorizerAspect {

  var dataMetadata: Metadata = null;
  var algorithm:LBSAlgorithm=null;
  @Around(value = "execution(* org.apache.spark.rdd.MapPartitionsRDD.compute(..)) && args(theSplit,context)", argNames = "jp,theSplit,context")
  def aroundAdvice_spark(jp: ProceedingJoinPoint, parition: Partition, context: TaskContext): AnyRef = {

  //  println("----------------------- Going through the Aspect ---------------------------------");

    val iterator = (jp.proceed(jp.getArgs()));

    if (sys.env.contains("BlockColumns")) {
        val blockCols = sys.env("BlockColumns");
        val metadataPath = blockCols.substring(blockCols.indexOf(']') + 1, blockCols.length());
        if (metadataPath != null && metadataPath.trim().length() > 3 && dataMetadata == null) {
          synchronized {
            if (dataMetadata == null) {
              val data = Source.fromFile(metadataPath).getLines().mkString("\n");
              if (data != null && data.trim().length() > 0) {
                dataMetadata = new DataReader().readMetadata(data);
              }
            }
          }
        }
      val columnBlockingIterator = new ColumnBlockingInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], sys.env("BlockColumns"), dataMetadata);

      println("Returning   ColumnBlockingInterruptibleIterator : " + columnBlockingIterator)
      return columnBlockingIterator;
    }
    else if (sys.env.contains("LBS")) {
      if(algorithm==null)
      {
      algorithm = new LBSAlgorithm(LBSMetadata.getInstance(), new LBSParameters(4,1200,300), LBSMetadata.getPopulation(),LBSMetadata.getZip());
        
      }
      return new LBSInterruptibleIterator(context,iterator.asInstanceOf[Iterator[_]],algorithm);
    }
    // if (context.getLocalProperty("PRIVILEDGE") != null) {
    val policy = getPolicy(context, jp, PointCutType.SPARK);
    if (policy != None) {
      val authorizedIterator = new AuthorizedInterruptibleIterator(context, iterator.asInstanceOf[Iterator[_]], policy.get.regex);
      println("Returning new iterator")
      // return iterator;
      return authorizedIterator
    }
    return iterator
  }

  def getPolicy(context: org.apache.spark.TaskContext, jp: org.aspectj.lang.ProceedingJoinPoint, pcType: Any): Option[Policy] = {
    var policy: Option[Policy] = None;
    val auth: Option[String] = Some(context.getLocalProperty("USER")) // Util.extractAuth(context)

    var path = extractPathForSpark(jp);
    if (path == null || path.trim().length() == 0) {
      path = extractPathForSparkSQL(jp);
    }
    policy = AccessMonitor.getPolicy(path, auth)
    policy
  }

  def extractPathForSpark(jp: org.aspectj.lang.ProceedingJoinPoint): String = {
    var pathFound = false;
    var path: String = "";
    breakable {
      for (argument <- jp.getArgs()) {
        for (field <- argument.getClass.getDeclaredFields) {
          if (field.getName.equalsIgnoreCase("inputSplit") || field.getName.equalsIgnoreCase("split")) {
            field.setAccessible(true)
            val fullPath = field.get(jp.getArgs()(0)).toString()
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
    path
  }
  def extractPathForSparkSQL(jp: org.aspectj.lang.ProceedingJoinPoint): String = {
    var pathFound = false;
    var path: String = "";
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
    path
  }
}