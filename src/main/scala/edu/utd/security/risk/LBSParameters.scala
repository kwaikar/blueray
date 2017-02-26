package edu.utd.security.risk

class LBSParameters (recordCost:Double,maxPublisherBenefit:Double,publishersLoss:Double) extends Serializable{
  
  def C():Double=
  {
    return recordCost;
  }
  
  def V():Double = {
    return maxPublisherBenefit;
  }
  
  def L():Double=
  {
    return publishersLoss;
  }
  
  /*def getAdversaryAttackCost():Double=
  {
    return adversaryAttackCost;
  }*/
}