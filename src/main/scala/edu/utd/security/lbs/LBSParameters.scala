package edu.utd.security.lbs

class LBSParameters (recordCost:Double,maxPublisherBenefit:Double) extends Serializable{
  
  def getRecordCost():Double=
  {
    return recordCost;
  }
  
  def getMaxPublisherBenefit():Double = {
    return maxPublisherBenefit;
  }
}