package de.dimajix.training.spark.jdbc

import java.sql.Timestamp
import java.util.Locale

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat


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
        StructField("air_temperature", DoubleType, true) ::
        StructField("wind_speed_quality", IntegerType, false) ::
        StructField("wind_speed", DoubleType, true) ::
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
    val airTemperature = row.substring(87,92).toDouble/10
    val windSpeedQuality = row.charAt(69) - '0'.toInt
    val windSpeed = row.substring(65,69).toDouble/10

    Row(date,time,timestamp,usaf,wban,airTemperatureQuality,airTemperature,windSpeedQuality,windSpeed)
  }
}
