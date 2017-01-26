package edu.utd.security.lbs

class LBSParameters (recordCost:Double,maxPublisherBenefit:Double,publishersLoss:Double, adversaryAttackCost:Double) extends Serializable{
  
  def getRecordCost():Double=
  {
    return recordCost;
  }
  
  def getMaxPublisherBenefit():Double = {
    return maxPublisherBenefit;
  }
  
  def getPublishersLossOnIdentification():Double=
  {
    return publishersLoss;
  }
  
  def getAdversaryAttackCost():Double=
  {
    return adversaryAttackCost;
  }
}