package edu.utd.security.blueray

import javax.crypto.SecretKey
import javax.crypto.Cipher
import java.util.Base64
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
}