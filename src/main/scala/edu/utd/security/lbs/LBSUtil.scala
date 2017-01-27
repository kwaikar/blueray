package edu.utd.security.lbs

object LBSUtil {
  def getMinMax(value: String): (Double, Double) = {
    if (value.contains("_")) {
      val range = value.split("_");
      val min = range(0).toDouble
      val max = range(1).toDouble
      (min, max)
    } else {
      (value.toDouble, value.toDouble)
    }
  }
}