package edu.utd.security.risk

/**
 * Class responsible for holding hierarchy for categorical values.
 */
class Category(value: String) extends Serializable {

  def value(): String =
    {
      return value.trim();
    }
  var children: List[Category] = List();
  var childrenString: List[String] = List(value);
  var leaves: List[String] = List();
  var min: Double = -1;
  var max: Double = -1;

  var map: scala.collection.mutable.HashMap[String, Int] = null;
  var revMap: scala.collection.mutable.HashMap[Int, String] = null;
  if (value.contains("_")) {
    val minMax = LBSUtil.getMinMax(value);
    min = minMax._1;
    max = minMax._2;
  }
  def getMin(): Double = {
    return min;
  }
  def getMax(): Double = {
    return max;
  }
  def hasChildren(): Boolean = {
    children.length > 0;
  }
  def addChildren(childrenCategory: Category) {
    this.children = this.children :+ childrenCategory;
    this.childrenString ++= childrenCategory.childrenString;
    if (childrenCategory.children.length == 0) {
      this.leaves = this.leaves :+ childrenCategory.value();
    } else {
      this.leaves ++= childrenCategory.leaves;
    }
  }

  def getValueAtIndex(key: Int): String =
    {
      return revMap.get(key).get;
    }
  def getIndexOfColumnValue(key: String): Int =
    {
      if (map == null || map.size == 0) {
        var index = 0;
        map = new scala.collection.mutable.HashMap();
        revMap = new scala.collection.mutable.HashMap();
        var queue = scala.collection.mutable.Queue[Category]();
        queue.enqueue(this);
        while (!queue.isEmpty) {
          val category = queue.dequeue();
          for (child <- category.children) {
            if (child.children.length == 0) {
              map.put(child.value().trim(), index)
              revMap.put(index, child.value().trim())
              index = index + 1;
            } else {
              queue.enqueue(child);
            }
          }
        }
      }
      //println("Checking "+key+" in  "+map.mkString(","));
      return map.get(key.trim()).get;
    }
  override def toString: String = {
    return value + "(" + value + "=" + childrenString.mkString + ")=>" + "[" + children.foreach { x => x.toString() } + "]";
  }
}
