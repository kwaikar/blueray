package edu.utd.security.blueray

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class SecurityTest {

  @Test
  def testEncryptDecrypt() =
    {
      val input = "hello Its me";
      val encryptedText = Security.encrypt(input)
      println(encryptedText)
      val decryptedText = Security.decrypt(encryptedText)
      println(decryptedText)
      assert(decryptedText.equalsIgnoreCase(input))
    }
}