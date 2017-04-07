package edu.utd.security.risk

import java.util.concurrent.ConcurrentHashMap







/**
 * Class responsible for holding details of column object.
 */
class Column(name: String, index: Int, colType: Char, isQuasiIdentifier: Boolean, rootCategory: Category, min: Double, max: Double, numUnique: Int) extends Serializable {
  def getName(): String = {
    return name;
  }
  def getIndex(): Int = {
    return index;
  }

  def getNumUnique(): Int = {
    return numUnique;
  }
  def getMin(): Double = {
    return min;
  }
  var MINMAX=min+"_"+max
 def getMinMax(): String= {
    return MINMAX;
  }
  def getMax(): Double = {
    return max;
  }

  def getColType(): Char = {
    return colType;
  }
  def getIsQuasiIdentifier(): Boolean = {
    return isQuasiIdentifier;
  }
  def getRootCategory(): Category = {
    return rootCategory;
  }
  override def toString: String = {
    if (rootCategory == null)
      return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + min + "<->" + max + "]";
    else
      return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + rootCategory.toString + "]";
  }
   
  /**
   * Given list of string values, this method finds the bottom most category that contains all elements containing given set.
   */
  def findCategory(columnValues: Array[String]): Category = {

    var category = rootCategory;
    var childFound = true;

    /**
     * Start from ancestor and Recurse until the parent node is found.
     */
    while (childFound) {

      if (category.children != null && category.children.size > 0) {
        childFound = false;
        val childrens = category.children.toArray
        for (i <- 0 to childrens.size - 1) {
          if (childrens(i).childrenString.intersect(columnValues).length == columnValues.length) {
            category = childrens(i);
            childFound = true;
          }
        }
        /**
         * If none of the children have all column values, it means that
         * current level itself is a common parent for all values.
         */
        if (!childFound) {
          return category;
        }

      } else {
        /**
         * Reached leaf - this is the bottommost level possible!
         */
        return category;
      }
    }
    return rootCategory;
  }

  var parentMap = new ConcurrentHashMap[String,Category](); 
  var catMap =  new ConcurrentHashMap[String,Category](); 
  
  def getParentCategory(childCategory: String): Category = {

    val parentVal =parentMap.get(childCategory);
    if(parentVal!=null)
    {
      return parentVal;
    }
    var category = rootCategory;
    var searchChild = true;
    var parent = rootCategory;
    /**
     * Start from ancestor and Recurse until the parent node is found.
     */
    while (searchChild && category.hasChildren()) {
      searchChild = false;
      val children = category.children.toArray
      for (i <- 0 to (children.size - 1)) {
        if (colType == 's') {
          if (children(i).childrenString.contains(childCategory)) {
            parent = category;
            category = children(i);
            searchChild = true;
          }
        } else {
          val minMax = LSHUtil.getMinMax(childCategory);

          if (((minMax._1 == minMax._2) && (minMax._1 >= children(i).getMin() && minMax._2 <= children(i).getMax())) ||
            ((minMax._1 != minMax._2) && (minMax._1 > children(i).getMin() && minMax._2 < children(i).getMax()))) {
            parent = children(i);
            category = children(i);
            searchChild = true;
          }
        }

      }
      if (!searchChild) {
        parentMap.put(childCategory,parent)
     //   println("Parent for"+childCategory+ " : Is "+parent);
        return parent;
      }
    }
        parentMap.put(childCategory,parent)
   // println("Parent for"+childCategory+ " : Is "+parent);
    return parent;
  }

  def getCategory(childCategory: String): Category = {

     val catVal =catMap.get(childCategory);
    if(catVal!=null)
    {
      return catVal;
    }
    
    var category = rootCategory;
    var searchChild = true;
    /**
     * Start from ancestor and Recurse until the parent node is found.
     */
    while (searchChild && category.hasChildren()) {
      searchChild = false;
      val children = category.children.toArray
      for (i <- 0 to (children.size - 1)) {
        if (colType == 's') {
          if (children(i).childrenString.contains(childCategory)) {
            category = children(i);
            searchChild = true;
          }
        } else {
          val minMax = LSHUtil.getMinMax(childCategory);
          if (minMax._1 > children(i).getMin() && minMax._2 < children(i).getMax()) {
            category = children(i);
            searchChild = true;
          }
        }
      }
      if (!searchChild) {
        catMap.put(childCategory,category)
       // println("Category for"+childCategory+ " : Is "+category);
        return category;
      }
    }
       // println("Category for"+childCategory+ " : Is "+category);
        catMap.put(childCategory,category)
    return category;
  }

}