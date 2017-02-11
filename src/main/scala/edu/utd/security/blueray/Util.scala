package edu.utd.security.blueray

import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.util.parsing.json.JSON

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import javax.crypto.spec.SecretKeySpec

object Util {
/*
  val secretKey = new SecretKeySpec("MY_SECRET_KEY_12".getBytes, "AES")*/
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
  var sc: SparkContext = SparkContext.getOrCreate(conf);

  def getSC(): SparkContext = {
    if (sc == null) {
      println("Created new context");
      sc = new SparkContext(conf)
    }
    sc;
  }

  def getFileAsString(path: String): String = {
    var sc: SparkContext = getSC()
    val value = sc.textFile(path).collect().mkString
    value
  }

  


  def storeStringAsFile(fileString: String, path: String) = {
    var sc = getSC();
    val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(path), sc.hadoopConfiguration)
    val os = fs.create(new Path(path))
    os.write(fileString.getBytes)

  }
  def splitLine(line: String) = {
    val splits = line.split("\\^");
    if (splits.size == 3)
      List(splits(0), splits(1), splits(2));
    else
      List(splits(0), splits(1), splits(2), splits(3));
  }

  def encrypt(plainText: String): String =
    {
      /*var cipher = Cipher.getInstance("AES");
      var plainTextByte: Array[Byte] = plainText.getBytes();
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      var encryptedByte: Array[Byte] = cipher.doFinal(plainTextByte);
      return Base64.getEncoder().encodeToString(encryptedByte);*/

      //Security.encrypt(plainText)
      plainText
    }
  /**
   * Commenting encryption decryption as it is currently not the requirement.
   */
  def decrypt(encryptedText: String): String = {
    /*    var cipher = Cipher.getInstance("AES");
    var encryptedTextByte: Array[Byte] = Base64.getDecoder().decode(encryptedText);
    cipher.init(Cipher.DECRYPT_MODE, secretKey);
    return (new String(cipher.doFinal(encryptedTextByte)));*/

    //Security.decrypt(encryptedText)

    encryptedText;
  }

  def extractAuth(context: org.apache.spark.TaskContext): Option[String] = {
    if (context.getLocalProperty("PRIVILEDGE") != null) {
      var auth = Util.decrypt(context.getLocalProperty("PRIVILEDGE"))
      Some(auth)
    } else {
      return None
    }
  }


  val BLOCKED_VALUE_WRAPPER = "-";


}


object PointCutType extends Enumeration {
  type PointCutType = String;
  val SPARK = "SPARK";
  val SPARKSQL = "SPARKSQL";
}
