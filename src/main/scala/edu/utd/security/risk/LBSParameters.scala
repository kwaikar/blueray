package edu.utd.security.risk

class LBSParameters (recordCost:Double,maxPublisherBenefit:Double,publishersLoss:Double) extends Serializable{
  
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
  
  /*def getAdversaryAttackCost():Double=
  {
    return adversaryAttackCost;
  }*/
}