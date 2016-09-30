package edu.utd.security.blueray

import java.util.Base64

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

 
 
object Util {

  val secretKey = new SecretKeySpec("MY_SECRET_KEY_12".getBytes, "AES")

  
  
  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }

  def encrypt(plainText: String): String =
    {
      var cipher = Cipher.getInstance("AES");
      var plainTextByte: Array[Byte] = plainText.getBytes();
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      var encryptedByte: Array[Byte] = cipher.doFinal(plainTextByte);
      return Base64.getEncoder().encodeToString(encryptedByte);
    }

  def decrypt(encryptedText: String): String = {
    var cipher = Cipher.getInstance("AES");
    var encryptedTextByte: Array[Byte] = Base64.getDecoder().decode(encryptedText);
    cipher.init(Cipher.DECRYPT_MODE, secretKey);
    return (new String(cipher.doFinal(encryptedTextByte)));

  }

  def extractAuth(context: org.apache.spark.TaskContext) = {
    println(context.getLocalProperty("PRIVILEDGE"))
    var auth = Util.decrypt(context.getLocalProperty("PRIVILEDGE"))
    println(" auth:" + auth);
    auth
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
          }
        else if (field.getName.equalsIgnoreCase("files")) {
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
    path
  }
}

object PointCutType extends Enumeration {
    type PointCutType = String;
    val SPARK = "SPARK";
    val SPARKSQL = "SPARKSQL";
  }