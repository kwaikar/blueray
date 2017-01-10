package edu.utd.security.mondrian

/**
 * Class responsible for holding details of column object.
 */
class Column(name: String, index: Int, colType: Char, isQuasiIdentifier: Boolean, rootCategory: Category) extends Serializable {
  def getName(): String = {
    return name;
  }
  def getIndex(): Int = {
    return index;
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
      return index + ":" + name + "=" + colType + "_" + isQuasiIdentifier + "[" + "]";
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
}