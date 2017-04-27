package edu.utd.security.risk

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer

/**
 * Class responsible for holding hierarchy for categorical values.
 */
class Category(value: String, par: Category) extends Serializable {

  val parent = par;
  def value(): String =
    {
      return value.trim();
    }
  var children: List[Category] = List();
  var childrenString: List[String] = List(value);
  var leaves: List[String] = List();
  var min: Double = -1;
  var max: Double = -1;

  var map: ConcurrentHashMap[String, Int] = null;
  var parentMap: ConcurrentHashMap[String, Int] = null;
  var revParentMap: ConcurrentHashMap[Int, Category] = null;

  var revMap: ConcurrentHashMap[Int, String] = null;
  if (value.contains("_")) {
    val minMax = LSHUtil.getMinMax(value);
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
      populateMapIfRequired();
      return revMap.get(key);
    }
  def getIndexOfColumnValue(key: String): Int =
    {
      populateMapIfRequired();
      // println("Looking for |"+key.trim()+"|")
      return map.get(key.trim());
    }
  def getParentIndexOfColumnValue(key: String): ListBuffer[Int] =
    {
      populateMapIfRequired();
      var list = ListBuffer[Int]();
      println(":::" + key + " :" + parentMap);
      val index = parentMap.get(key.trim());
      list.+=(index)
      var value = revParentMap.get(index);
      while (value.parent != null) {
        list.+=(parentMap.get(value.parent.value().trim()))
        value = value.parent;
      }
      return list;
    }
  def populateMapIfRequired() = {
    if (map == null || map.size == 0) {
      var index = 0;
      map = new ConcurrentHashMap();
      revMap = new ConcurrentHashMap();
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
    if (parentMap == null || parentMap.size == 0) {
      var index = 0;
      parentMap = new ConcurrentHashMap();
      revParentMap = new ConcurrentHashMap();
      var queue = scala.collection.mutable.Queue[Category]();
      queue.enqueue(this);
      while (!queue.isEmpty) {
        val category = queue.dequeue();
        if (!parentMap.containsKey(category.value().trim())) {
          parentMap.put(category.value().trim(), index)
          revParentMap.put(index, category)
          index = index + 1;
          if (category.children.size > 0) {
            for (child <- category.children) {
              queue.enqueue(child);
            }
          }
        }
      }
    }
  }
  override def toString: String = {
    return value + "(" + value + "=" + childrenString.mkString + ")=>" + "[" + children.foreach { x => x.toString() } + "]";
  }
}
