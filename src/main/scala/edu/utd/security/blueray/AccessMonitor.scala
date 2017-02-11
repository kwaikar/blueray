package edu.utd.security.blueray

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import java.net.URLEncoder
import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Singleton object for implementing Access Control in Spark
 */
object AccessMonitor {

   val useRESTAPI = true;

  // val logger = Logger(LoggerFactory.getLogger(this.getClass))
  var policies: HashMap[String, HashSet[Policy]] = new scala.collection.mutable.HashMap
  var policiesLoaded = false;
 
  /**
   * Register policy mechanism for enforcing new policy
   */
  def enforcePolicy(policy: Policy) {
     if (useRESTAPI) {
      enforcePolicyOnRESTEndPoint(policy.resourcePath, policy.priviledge, policy.regex);
     } else {
      policy.priviledge = policy.priviledge
      var policiesSet: HashSet[Policy] = if (policies.get(policy.resourcePath) != None) (policies.get(policy.resourcePath).get) else (new HashSet[Policy]);
      policiesSet.add(policy)
      println("Added policy:" + policy);
      policies.put(policy.resourcePath, policiesSet)
    } 
  }
  def deRegisterPolicy(policy: Policy) {
     if (useRESTAPI) {
      deregisterPolicyOnRESTEndPoint(policy.resourcePath, policy.priviledge, policy.regex);
     } else {
      var policiesSet: Option[HashSet[Policy]] = policies.get(policy.resourcePath)
      if (policiesSet != None) {
        for (entry <- policiesSet.get) {
          if (entry.regex.equalsIgnoreCase(policy.regex) && entry.resourcePath.equalsIgnoreCase(policy.resourcePath) && entry.regex.equalsIgnoreCase(policy.regex)) {
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
    println("Policies deregistered:" + policies)  
  }
   def loadPolicies() {
    if (!policiesLoaded) {
      println("Reading policies from path : " + getPolicyPath())
      val lines = Source.fromFile(getPolicyPath()).getLines();
      for (line <- lines) {

        val arr = line.split(",");
        var regex = arr(0);
        if (arr(0).startsWith("\"")) {
          regex = arr(0).replaceAll("\"", "");
        }
        println("Final: " + arr(0) + " : " + regex);
        enforcePolicy(new Policy(arr(2), (arr(1)), regex));
      }
      println("Policies read");
      policiesLoaded = true;
    }
  } 
  /**
   * Returns policy from map based on authorization
   */
  def getPolicy(path: String, priviledgeRestriction: Option[String]): Option[Policy] =
    {
       if (useRESTAPI) { 
        return getPolicyFromEndPoint(path, priviledgeRestriction.get);

       } else {
        println("going through======================" + path);
        loadPolicies();
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
                if (policy.priviledge.equalsIgnoreCase(priviledgeRestriction.get)) {
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
        println("Returning policy" + policyToBeReturned)
        return policyToBeReturned
      } 
    }

  def getPolicyFromEndPoint(filePath: String, priviledge: String): Option[Policy] = {
    println("-->"+getURL()+ " :"+filePath+"-"+priviledge)
    val output = getURLAsString(getURL() + "/policy?priviledge=" + priviledge.trim() + "&filePath=" + filePath.trim())
    println(filePath+ " =>"+output)
    if (!output.contains("No Policy")) {
      val policy = extractPolicy(output);
      println("Returing policy:" + policy)
      policy
    } else {
      return None;
    }
  }
  
  def getPolicyPath():String={
    return sys.env("BLUERAY_POLICIES_PATH")
  return "/data/kanchan/policies.csv";
  }
  
  def getURL():String={
    return sys.env("POLICYMANAGER_END_POINT");
  }
  
   def getURLAsString(url: String): String = {
    val html = Source.fromURL(url)
    html.mkString
  }

  def enforcePolicyOnRESTEndPoint(filePath: String, priviledge: String, regex: String) {
    val output = getURLAsString(getURL() + "/enforcePolicy?priviledge=" + priviledge.trim() + "&filePath=" + URLEncoder.encode(filePath.trim()) + "&regex=" + URLEncoder.encode(regex.trim()))
    println(output)
  }
  def deregisterPolicyOnRESTEndPoint(filePath: String, priviledge: String, regex: String) {
    val output = getURLAsString(getURL() + "/deregisterPolicy?priviledge=" + priviledge .trim()+ "&filePath=" + URLEncoder.encode(filePath.trim()) + "&regex=" + URLEncoder.encode(regex.trim()))
    println(output)
  }

  def extractPolicy(json: String): Option[Policy] = {
    var policy: Option[Policy] = None;
    val policyJson = JSON.parseFull(json);
    if (policyJson != None) {
      val filePath = policyJson match {
        case Some(m: Map[String, Any]) => m("filePath") match {
          case s: String => s
        }
      }

      val priviledge = policyJson match {
        case Some(m: Map[String, Any]) => m("priviledge") match {
          case s: String => s
        }
      }

      val regex = policyJson match {
        case Some(m: Map[String, Any]) => m("regex") match {
          case s: String => s
        }
      }
      return Some(new Policy(filePath, priviledge, regex));

    }
    return policy;
  }
}
