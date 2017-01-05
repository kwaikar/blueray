package edu.utd.security.blueray

/**
 * Policy Class to be used for enforcing policies
 */
class Policy(resource: String, restriction: String, expr: String) {
  var resourcePath: String = resource;
  var filterExpression = expr;
  var priviledgeRestriction = restriction;

  override def toString(): String = {
    resourcePath + " : " + filterExpression + " - " + priviledgeRestriction
  }
}