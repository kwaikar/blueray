package edu.utd.security.lbs

class LBSParameters (recordCost:Double) extends Serializable{
  
  def getRecordCost():Double=
  {
    return recordCost;
  }
}