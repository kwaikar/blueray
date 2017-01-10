package edu.utd.security.mondrian

 /**
   * class used for sharing output of dimension with the calling method.
   */
  class Dimensions(dimensionValue: Int, minValue: Double, medianValue: Double, maxValue: Double, leftArray: Array[String], rightArray: Array[String]) extends Serializable {
    def dimension(): Int =
      {
        dimensionValue
      }
    def min(): Double =
      {
        minValue
      }
    def median(): Double =
      {
        this.medianValue
      }
    def max(): Double =
      {
        maxValue
      }

    def leftSet(): Array[String] =
      {
        leftArray
      }

    def rightSet(): Array[String] =
      {
        rightArray
      }
  }