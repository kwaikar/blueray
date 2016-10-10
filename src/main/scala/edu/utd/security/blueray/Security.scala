package edu.utd.security.blueray

import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.PBEParameterSpec

object Security {

  var salt: Array[Byte] = Array[Byte](
    0xc7.toByte, 0x73.toByte, 0x21.toByte, 0x8c.toByte,
    0x7e.toByte, 0xc8.toByte, 0xee.toByte, 0x99.toByte)

  val password = "ADMIN_1_PASSWORD"

  def main(args: Array[String]): Unit = {
    val encryptedText = Security.encrypt("hello Its me")
    val decryptedText = Security.decrypt(encryptedText)
  }

  def generateAndStoreMasterKey(admin1Password:String, admin2Password:String)
  {
    // This method would write encryption key to hdfs file
  }
  def decrypt(encryptedText: String): String =
    {

      var count = 20;
      // Create PBE parameter set

      val pbeKey= getSecretEncryptionKey();

      // Create PBE Cipher
      var pbeParamSpec: PBEParameterSpec = new PBEParameterSpec(salt, count)
      var pbeCipher: Cipher = Cipher.getInstance("PBEWithMD5AndDES");
      pbeCipher.init(Cipher.DECRYPT_MODE, pbeKey, pbeParamSpec);

      // Encrypt the cleartext
      var deCipheredText: Array[Byte] = pbeCipher.doFinal(Base64.getDecoder.decode(encryptedText));
      new String(deCipheredText)

    }

  def getSecretEncryptionKey() = { 

    val pbeKeySpec: PBEKeySpec = new PBEKeySpec(password.toCharArray());
    val keyFac: SecretKeyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
    val pbeKey: SecretKey = keyFac.generateSecret(pbeKeySpec);
    pbeKey
  }
  def encrypt(plainText: String): String =
    {

      // Salt

      // Iteration count
      var count = 20;

      // Create PBE parameter set
      val pbeKey= getSecretEncryptionKey();

      // Create PBE Cipher
      var pbeCipher: Cipher = Cipher.getInstance("PBEWithMD5AndDES");

      // Initialize PBE Cipher with key and parameters
      var pbeParamSpec: PBEParameterSpec = new PBEParameterSpec(salt, count)
      pbeCipher.init(Cipher.ENCRYPT_MODE, pbeKey, pbeParamSpec);

      var cipherText: Array[Byte] = pbeCipher.doFinal(plainText.getBytes);
      Base64.getEncoder().encodeToString(cipherText)
    }
}