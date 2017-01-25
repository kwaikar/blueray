package edu.utd.security.lbs


  /**
   * Class responsible for holding hierarchy for categorical values.
   */
  class Category(value: String) extends Serializable {
    def value(): String =
      {
        return value;
      }
    var children: List[Category] = List();
    var childrenString: List[String] = List(value);
    def hasChildren():Boolean={
      children.length>0;
    }
    def addChildren(childrenCategory: Category) {
      this.children = this.children :+ childrenCategory;
      this.childrenString = this.childrenString :+ childrenCategory.value();
      this.childrenString ++= childrenCategory.childrenString;
    }
    override def toString: String = {
      return value + "(" + value + "=" + childrenString.mkString + ")=>" + "[" + children.foreach { x => x.toString() } + "]";
    }
  }
