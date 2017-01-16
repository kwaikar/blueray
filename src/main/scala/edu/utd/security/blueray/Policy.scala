package edu.utd.security.blueray

/**
 * Policy Class to be used for enforcing policies
 */
class Policy(resource: String, restriction: String, expr: String) {
  var resourcePath: String = resource;
  var priviledge = restriction;
  var regex = expr;

  override def toString(): String = {
    resourcePath + " : " + regex + " - " + priviledge
  }
}