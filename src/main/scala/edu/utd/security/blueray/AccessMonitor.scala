package edu.utd.security.blueray

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

/**
 * Singleton object for implementing Access Control in Spark
 */
object AccessMonitor {

  // val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val policies: HashMap[String, HashSet[Policy]] = new scala.collection.mutable.HashMap
  /**
   * Register policy mechanism for enforcing new policy
   */
  def enforcePolicy(policy: Policy) {
    policy.priviledgeRestriction = Util.decrypt(policy.priviledgeRestriction)
    var policiesSet: HashSet[Policy] = if (policies.get(policy.resourcePath) != None) (policies.get(policy.resourcePath).get) else (new HashSet[Policy]);
    policiesSet.add(policy)

    policies.put(policy.resourcePath, policiesSet)
    //  logger.debug("registered policy"+policy)
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
  }
  /**
   * Returns policy from map based on authorization
   */
  def getPolicy(path: String, priviledgeRestriction: Option[String]): Option[Policy] =
    {
      var policyToBeReturned: Option[Policy] = None;

      for (hashSet <- policies) {
        breakable {
          if (hashSet._1.startsWith(path.trim())) {
            if (priviledgeRestriction == None) {
              return Some(new Policy(path, "", ""))
            }
            for (policy <- hashSet._2) {
              if (policy.priviledgeRestriction.equalsIgnoreCase(priviledgeRestriction.get)) {
                policyToBeReturned = Some(policy);
                break;
              }
            }

            return Some(new Policy(path, "", ""))
          }
        }
      }
      return policyToBeReturned
    }
}
