package edu.utd.security.blueray

import scala.collection.mutable.HashMap
import com.typesafe.scalalogging._
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
    policies.remove(policy.resourcePath)
  }
  /**
   * Returns policy from map based on authorization
   */
  def getPolicy(path: String, priviledgeRestriction: String): Option[Policy] =
    {
      var policyToBeReturned: Option[Policy] = None;

      for (hashSet <- policies) {
        breakable {
          if (hashSet._1.startsWith(path.trim())) {
            for (policy <- hashSet._2) {
              if (policy.priviledgeRestriction.equalsIgnoreCase(priviledgeRestriction)) {
                policyToBeReturned = Some(policy);
                break;
              }
            }
          }
        }
      }
      return policyToBeReturned
    }
}
