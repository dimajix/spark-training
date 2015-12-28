import java.sql.Timestamp
import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.TimestampType
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

object StationData extends Serializable {
  def schema = {
    StructType(
      StructField("usaf", StringType, false) ::
        StructField("wban", StringType, false) ::
        StructField("name", StringType, false) ::
        StructField("country", StringType, false) ::
        StructField("fips", StringType, false) ::
        StructField("state", StringType, false) ::
        StructField("call", StringType, false) ::
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


object WeatherData extends Serializable {
  @transient
  private final val TIMESTAMP_FORMAT = "yyyyMMddHHmm"
  @transient
  private final val timestampFormatter = DateTimeFormat.forPattern(TIMESTAMP_FORMAT).withZone(DateTimeZone.UTC).withLocale(Locale.US)

  def schema = {
    StructType(
      StructField("date", StringType, false) ::
        StructField("time", StringType, false) ::
        StructField("timestamp", TimestampType, false) ::
        StructField("usaf", StringType, false) ::
        StructField("wban", StringType, false) ::
        StructField("air_temperature_quality", IntegerType, false) ::
        StructField("air_temperature", FloatType, false) ::
        StructField("wind_speed_quality", IntegerType, false) ::
        StructField("wind_speed", FloatType, false) ::
        Nil
    )
  }
  def extract(row:String) = {
    val date = row.substring(15,23)
    val time = row.substring(23,27)
    val timestamp = new Timestamp(timestampFormatter.parseDateTime(date + time).getMillis())
    val usaf = row.substring(4,10)
    val wban = row.substring(10,15)
    val airTemperatureQuality = row.charAt(92).toInt - '0'.toInt
    val airTemperature = row.substring(87,92).toFloat/10
    val windSpeedQuality = row.charAt(69) - '0'.toInt
    val windSpeed = row.substring(65,69).toFloat/10

    Row(date,time,timestamp,usaf,wban,airTemperatureQuality,airTemperature,windSpeedQuality,windSpeed.toFloat)
  }
}


val raw_weather = sc.textFile("weather/2011")
val weather_rdd = raw_weather.map(WeatherData.extract)
val weather = sqlContext.createDataFrame(weather_rdd, WeatherData.schema)

val ish_raw = sc.textFile("weather/ish")
val ish_head = ish_raw.first
val ish_rdd = ish_raw
  .filter(_ != ish_head)
  .map(StationData.extract)
val ish = sqlContext.createDataFrame(ish_rdd, StationData.schema)


weather.take(10).foreach(println)


val joined_weather = weather.join(ish, weather("usaf") === ish("usaf") && weather("wban") === ish("wban")).withColumn("year", weather("date").substr(0,4))
joined_weather.printSchema


joined_weather.groupBy("country", "year")
    .agg(min("air_temperature"), max("air_temperature"))
    .take(10)
    .foreach(println)


weather.join(ish, weather("usaf") === ish("usaf") && weather("wban") === ish("wban"))
  .withColumn("year", weather("date").substr(0,4))
  .groupBy("country", "year")
  .agg(
    min(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(9999)),
    max(when(col("air_temperature_quality") === lit(1), col("air_temperature")).otherwise(-9999))
  )
  .take(100).foreach(println)


weather.join(ish, weather("usaf") === ish("usaf") && weather("wban") === ish("wban"))
  .withColumn("year", weather("date").substr(0,4))
  .groupBy("country", "year")
  .agg(
    min(callUDF((q:Int,t:Float) => if (q == 1) t else 9999.toFloat, FloatType, col("air_temperature_quality"), col("air_temperature"))),
    max(callUDF((q:Int,t:Float) => if (q == 1) t else -9999.toFloat, FloatType, col("air_temperature_quality"), col("air_temperature")))
  )
  .take(100).foreach(println)