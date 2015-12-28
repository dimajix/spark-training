package de.dimajix.training.spark.weather

import org.apache.spark.sql.Row
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
      StructField("fips", StringType, false) ::
      StructField("state", StringType, false) ::
      StructField("kall", StringType, false) ::
      StructField("latitude", IntegerType, true) ::
      StructField("longitude", IntegerType, true) ::
      StructField("elevation", IntegerType, true) ::
      StructField("date_begin", StringType, false) ::
      StructField("date_end", StringType, true) ::
      Nil
    )
  }
  def extract(row:String) = {
    def getInt(str:String) : Integer = {
      if (str.isEmpty)
        return null
      else
        return str.toInt
    }
    val columns = row.split(",").map(_.replaceAll("\"",""))
    val latitude = getInt(columns(7))
    val longitude = getInt(columns(8))
    val elevation = getInt(columns(9))
    Row(columns(0),columns(1),columns(2),columns(3),columns(4),columns(5),columns(6),latitude,longitude,elevation,columns(10),columns(11))
  }
}
