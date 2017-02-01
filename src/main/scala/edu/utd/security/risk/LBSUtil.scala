package edu.utd.security.risk

object LBSUtil {
  def getMinMax(value: String): (Double, Double) = {
    if (value.contains("_")) {
      val range = value.split("_");
      val min = range(0).toDouble
      val max = range(1).toDouble
      (min, max)
    } else {
      (value.toDouble, value.toDouble)
    }
  }
  def getColumnStartCounts(metadata: Metadata): Array[Int] = {
    var nextStartCount = 0;
    var columnStartCounts: Array[Int] = Array(metadata.numColumns());
    var index = 0;

    for (i <- 0 to metadata.numColumns() - 1) {
      val column = metadata.getMetadata(i).get;
      if (column.getIsQuasiIdentifier()) {
        if (column.getColType() == 's') {
          columnStartCounts(index) = nextStartCount;
          index = index + 1;
          nextStartCount = nextStartCount + column.getNumUnique();
        } else {
          columnStartCounts(index) = nextStartCount;
          index = index + 1;
          nextStartCount = nextStartCount + 1;
        }
      }
    }
    return columnStartCounts;
  }

  def getTotalNewColumns(metadata: Metadata): Int = {
    var totalCounts = 0;
    for (i <- 0 to metadata.numColumns() - 1) {
      val column = metadata.getMetadata(i).get; if (column.getIsQuasiIdentifier()) {
        if (column.getColType() == 's') {
          totalCounts = totalCounts + column.getNumUnique();
        } else {
          totalCounts = totalCounts + 1;
        }
      }
    }
    return totalCounts;
  }
  
  	def extractRow(metadata:Metadata ,columnStartCounts : Array[Int], values:Map[Int,String],normalize:Boolean):Array[Double]= {
		var  row:Array[Double] = Array(getTotalNewColumns(metadata));
		var index = 0;

    for (i <- 0 to metadata.numColumns() - 1) {
      val column = metadata.getMetadata(i).get;
      if (column.getIsQuasiIdentifier()) {
        if (column.getColType() == 's') {
  				row((columnStartCounts(index) + column.getIndex((String) ds[index]))) = 1.0;
        }
        else
        {
          if(normalize)
  				{
  					row(columnStartCounts(index)) =((values.get(index).get.toDouble)- column.getMin())/(column.getMax()-column.getMin());
  				}
  				else
  				{
  					row(columnStartCounts(index)) = values.get(index).get.toDouble;	
  				}
			}
			index =index+1;
		}
    }
		return row;
	}
}
