package de.dimajix.training.spark.jdbc

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


object StationData extends Serializable {
  def schema = {
    StructType(
      StructField("usaf", StringType, false) ::
        StructField("wban", StringType, false) ::
        StructField("name", StringType, false) ::
        StructField("country", StringType, false) ::
        StructField("state", StringType, false) ::
        StructField("icao", StringType, false) ::
        StructField("latitude", FloatType, true) ::
        StructField("longitude", FloatType, true) ::
        StructField("elevation", FloatType, true) ::
        StructField("date_begin", StringType, false) ::
        StructField("date_end", StringType, true) ::
        Nil
    )
  }
  def extract(row:String) = {
    def getFloat(str:String) : Float = {
      if (str.isEmpty)
        return Float.NaN
      else if (str(0) == '+')
        return str.substring(1).toFloat
      else
        return str.toFloat
    }
    val columns = row.split(",").map(_.replaceAll("\"",""))
    val latitude = getFloat(columns(6))
    val longitude = getFloat(columns(7))
    val elevation = getFloat(columns(8))
    Row(columns(0),columns(1),columns(2),columns(3),columns(4),columns(5),latitude,longitude,elevation,columns(9),columns(10))
  }
}
