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
    var policiesSet: HashSet[Policy] = if (policies.get(policy.priviledgeRestriction) != None) (policies.get(policy.priviledgeRestriction).get) else (new HashSet[Policy]);
    policiesSet.add(policy)

    policies.put(policy.priviledgeRestriction, policiesSet)
    //  logger.debug("registered policy"+policy)
  }
  def deRegisterPolicy(policy: Policy) {
    policies.remove(policy.priviledgeRestriction)
  }
  /**
   * Returns policy from map based on authorization
   */
  def getPolicy(path: String, priviledgeRestriction: String): Option[Policy] =
    {
      var set: Option[HashSet[Policy]] = policies.get(priviledgeRestriction.trim());
      var policyToBeReturned: Option[Policy] = None;
      if (set != None) {
        var hashSet: HashSet[Policy] = set.get;
        breakable {
          for (policy <- hashSet) {
            if (policy.resourcePath.startsWith(path.trim())) {
              policyToBeReturned = Some(policy);
              break;
            }
          }
        }
      }
      return policyToBeReturned
    }
}
