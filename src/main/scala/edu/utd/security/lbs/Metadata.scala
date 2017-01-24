package edu.utd.security.common 

/**
   * Class responsible for holding metadata of columns.
   */
  class Metadata(columnMetadata: Map[Int, Column]) extends Serializable {

    def getMetadata(columnId: Int): Option[Column] = {
      return columnMetadata.get(columnId);
    }
    def numColumns(): Int = {
      return columnMetadata.size;
    }
    override def toString: String = {
      return columnMetadata.mkString;
    }
  }