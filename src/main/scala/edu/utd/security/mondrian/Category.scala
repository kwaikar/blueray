package edu.utd.security.mondrian


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
    def addChildren(childrenCategory: Category) {
      this.children = this.children :+ childrenCategory;
      this.childrenString = this.childrenString :+ childrenCategory.value();
      this.childrenString ++= childrenCategory.childrenString;
    }
    override def toString: String = {
      return value + "(" + value + "=" + childrenString.mkString + ")=>" + "[" + children.foreach { x => x.toString() } + "]";
    }
  }
