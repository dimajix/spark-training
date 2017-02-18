package de.dimajix.training.spark.weather

object StationData {
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
                        latitude:Float,
                        longitude:Float,
                        elevation:Float,
                        date_begin:String,
                        date_end:String
                      )
{
}
