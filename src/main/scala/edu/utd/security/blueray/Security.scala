package edu.utd.security.blueray

import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.PBEParameterSpec
import javax.crypto.spec.IvParameterSpec
import java.security.spec.AlgorithmParameterSpec
import java.security.AlgorithmParameters

object Security {

  var cipherEncrypt: Cipher = _;
  val ALGORITHM = "PBEWithHmacSHA256AndAES_128";
  var salt: Array[Byte] = Array[Byte](
    0xc7.toByte, 0x73.toByte, 0x21.toByte, 0x8c.toByte,
    0x7e.toByte, 0xc8.toByte, 0xee.toByte, 0x99.toByte)

  val pbeParamSpec: PBEParameterSpec = new PBEParameterSpec(salt, 10)

  val password = "ADMIN_1_PASSWORD"

  def main(args: Array[String]): Unit = {
    val encryptedText = Security.encrypt("hello Its me")
    val decryptedText = Security.decrypt(encryptedText)
  }

  def generateAndStoreMasterKey(admin1Password: String, admin2Password: String) {
    // This method would write encryption key to hdfs file
  }
  def decrypt(encryptedText: String): String =
    {

      val pbeKey = getSecretEncryptionKey();
      var pbeCipherDecrypt: Cipher = Cipher.getInstance(pbeKey.getAlgorithm);
      pbeCipherDecrypt.init(Cipher.DECRYPT_MODE, pbeKey, cipherEncrypt.getParameters);
      val encryptedBytes = Base64.getDecoder.decode(encryptedText);
      var deCipheredText: Array[Byte] = pbeCipherDecrypt.doFinal(encryptedBytes);
      println("--->" + new String(deCipheredText))
      new String(deCipheredText)
    }

  def getSecretEncryptionKey() = {
    val pbeKeySpec: PBEKeySpec = new PBEKeySpec(password.toCharArray());
    val keyFac: SecretKeyFactory = SecretKeyFactory.getInstance(ALGORITHM);
    val pbeKey: SecretKey = keyFac.generateSecret(pbeKeySpec);
    pbeKey
  }

  def encrypt(plainText: String): String =
    {
      val pbeKey = getSecretEncryptionKey();
      if (cipherEncrypt == null) {
        cipherEncrypt = Cipher.getInstance(pbeKey.getAlgorithm);
        cipherEncrypt.init(Cipher.ENCRYPT_MODE, pbeKey, pbeParamSpec);
      }
      var cipherText: Array[Byte] = cipherEncrypt.doFinal(plainText.getBytes);
      println("Encrypted:" + Base64.getEncoder().encodeToString(cipherText))
      Base64.getEncoder().encodeToString(cipherText)
    }
}