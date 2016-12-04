package edu.utd.security.blueray

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

/**
 * Singleton object for implementing Access Control in Spark
 */
object AccessMonitor {

  // val logger = Logger(LoggerFactory.getLogger(this.getClass))
   var policies: HashMap[String, HashSet[Policy]] = new scala.collection.mutable.HashMap
  /**
   * Register policy mechanism for enforcing new policy
   */
  def enforcePolicy(policy: Policy) {
    policy.priviledgeRestriction = Util.decrypt(policy.priviledgeRestriction)
    var policiesSet: HashSet[Policy] = if (policies.get(policy.resourcePath) != None) (policies.get(policy.resourcePath).get) else (new HashSet[Policy]);
    policiesSet.add(policy)

    policies.put(policy.resourcePath, policiesSet)
  }
  def deRegisterPolicy(policy: Policy) {
    var policiesSet: Option[HashSet[Policy]] = policies.get(policy.resourcePath)
    if (policiesSet != None) {
      for (entry <- policiesSet.get) {
        if (entry.filterExpression.equalsIgnoreCase(policy.filterExpression) && entry.resourcePath.equalsIgnoreCase(policy.resourcePath) && entry.filterExpression.equalsIgnoreCase(policy.filterExpression)) {
          policiesSet.get.remove(entry)
          if (policiesSet.get.size > 0) {
            policies.put(policy.resourcePath, policiesSet.get);
          } else {
            policies.remove(entry.resourcePath)
          }
        }
      }
    }
    println("Policies deregistered:"+policies)
  }
  /**
   * Returns policy from map based on authorization
   */
  def getPolicy(path: String, priviledgeRestriction: Option[String]): Option[Policy] =
    {
       if(AccessMonitor.policies.size==0){
         for (line <- Source.fromFile("/home/kanchan/policies.csv").getLines()) {
         
          val arr=line.split(",");
          println(arr.length+"|"+arr(0))
           AccessMonitor.enforcePolicy(new Policy(arr(2),Util.encrypt(arr(1)),(arr(0))));
          }
         println("Policiies"+policies);
       }
       
      var policyToBeReturned: Option[Policy] = None;

      for (hashSet <- policies) {
        breakable {
          //println("path.trim:" + path.trim())
          if (hashSet._1.startsWith(path.trim())) {
            if (priviledgeRestriction == None) {
             // println("policyToBeReturned:" + "New")
              return Some(new Policy(path, "", ""))
            }
            for (policy <- hashSet._2) {
              if (policy.priviledgeRestriction.equalsIgnoreCase(priviledgeRestriction.get)) {
                policyToBeReturned = Some(policy);
              //  println("policyToBeReturned:" + policyToBeReturned)
                break;
              }
            }
            //println("returning some")
            return Some(new Policy(path, "", ""))
          }
        }
      }
      return policyToBeReturned
    }
}
