package  edu.utd.security.blueray

class Policy(resource: String, filterValue: String) {
  var resourcePath: String = resource;
  var filterExpression = filterValue;
}