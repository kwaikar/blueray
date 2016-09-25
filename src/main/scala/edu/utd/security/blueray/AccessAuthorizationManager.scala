package edu.utd.security.blueray

import scala.collection.mutable.HashMap
import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory

/**
 * Singleton object for implementing Access Control in Spark
 */
object AccessAuthorizationManager {
 

  val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val policies: HashMap[String, Policy] = new scala.collection.mutable.HashMap

  /**
   * Register policy
   */
  def registerPolicy(policy: Policy) {
    policies.put(policy.resourcePath, policy)
    logger.debug("registered policy"+policy)
  }
  def deRegisterPolicy(policy: Policy) {
    policies.remove(policy.resourcePath)
  }
  /**
   * Returns policy
   */
  def getPolicy(path: String): Option[Policy] =
    {
      policies.get(path)
    }
}
