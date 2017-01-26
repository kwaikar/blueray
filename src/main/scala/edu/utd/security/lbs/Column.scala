package edu.utd.security.lbs 


/**
 * Class responsible for holding details of column object.
 */
class Column(name: String, index: Int, colType: Char, isQuasiIdentifier: Boolean, rootCategory: Category,min:Double,max:Double) extends Serializable {
  def getName(): String = {
    return name;
  }
  def getIndex(): Int = {
    return index;
  }
  
  def getMin(): Double= {
    return min;
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
      return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" +min+"<->"+max+ "]";
    else
      return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + rootCategory.toString + "]";
  }
  def depth():Int=
  {
     if(colType =='s')
      {
       return depth(rootCategory);
      }
     else
     {
       // Add code for calculating numeric depth
       return 1;
     }
  }
  def depth(category:Category):Int={
      var depthValue=0;    
     
      if (category.children != null && category.children.size > 0) {
        
        for (i <- 0 to category.children.size - 1) {
          var childDepth = depth(category.children(i));
          if(childDepth>depthValue)
          {
            depthValue = childDepth;
          }
        }
      }
      else
      {
        return 1;
      }
    return depthValue;
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
  
  /**
   * This method should implement logic for getting higher range for given value.
   */
  def getParentRange (value:Double):String={
    return value.toString();
  }
  
   def getParentCategory(childCategory: String): Category = {

    var category = rootCategory;
    var searchChild=true;
    var parent = rootCategory;
    /**
     * Start from ancestor and Recurse until the parent node is found. 
     */
    while (searchChild && category.hasChildren()) {

        val children = category.children.toArray
        for (i <- 0 to children.size - 1) {
          if (children(i).childrenString.contains(childCategory)) {
            parent =category;
            category = children(i);
            searchChild=true;
          }
        }
        if(!searchChild)
        {
          return parent;
        }
    }
    return parent;
  }
   
   
   def getCategory(categoryString: String): Category = {

    var category = rootCategory;
    var searchChild=true;
    /**
     * Start from ancestor and Recurse until the node is found. 
     */
    while (searchChild && category.hasChildren()) {

        val children = category.children.toArray
        for (i <- 0 to children.size - 1) {
          if (children(i).childrenString.contains(categoryString)) {
            category = children(i);
            searchChild=true;
          }
        }
        if(!searchChild)
        {
          return category;
        }
    }
    return category;
  }
}