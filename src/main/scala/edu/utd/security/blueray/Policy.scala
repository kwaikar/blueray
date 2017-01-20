package edu.utd.security.blueray

/**
 * Policy Class to be used for enforcing policies
 */
class Policy(resourceString: String, priviledgeString: String, regexString: String) {
  var resourcePath: String = resourceString;
  var priviledge = priviledgeString;
  var regex = regexString;

  override def toString(): String = {
    resourcePath + " : " + regex + " - " + priviledge
  }
}