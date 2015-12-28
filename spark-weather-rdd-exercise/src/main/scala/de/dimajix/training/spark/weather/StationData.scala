package de.dimajix.training.spark.weather

object StationData {
  def extract(row:String) = {
    def getInt(str:String) : Integer = {
      if (str.isEmpty)
        return 0
      else
        return str.toInt
    }
    val columns = row.split(",").map(_.replaceAll("\"",""))
    val latitude = getInt(columns(6))
    val longitude = getInt(columns(7))
    val elevation = getInt(columns(8))
    StationData(columns(0),columns(1),columns(2),columns(3),columns(4),columns(5),latitude,longitude,elevation,columns(9),columns(10))
  }
}

/**
  * Created by kaya on 05.12.15.
  */
case class StationData(
  usaf:String,
  wban:String,
  name:String,
  country:String,
  state:String,
  icao:String,
  latitude:Int,
  longitude:Int,
  elevation:Int,
  date_begin:String,
  date_end:String
)
{
}