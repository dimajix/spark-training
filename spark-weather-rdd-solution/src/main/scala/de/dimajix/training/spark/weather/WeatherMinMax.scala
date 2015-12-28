package de.dimajix.training.spark.weather

/**
  * Created by kaya on 05.12.15.
  */
case class WeatherMinMax(
                          minTemperature:Float = 99999,
                          maxTemperature:Float = -99999,
                          minWindSpeed:Float = 99999,
                          maxWindSpeed:Float = -99999
                        ) {
  def reduce(other:WeatherMinMax) = {
    val minT = minTemperature.min(other.minTemperature)
    val maxT = maxTemperature.max(other.maxTemperature)
    val minW = minWindSpeed.min(other.minWindSpeed)
    val maxW = maxWindSpeed.max(other.maxWindSpeed)
    WeatherMinMax(minT,maxT,minW,maxW)
  }
  def reduce(other:WeatherData) = {
    val minT = if(other.validTemperature) minTemperature.min(other.temperature) else minTemperature
    val maxT = if(other.validTemperature) maxTemperature.max(other.temperature) else maxTemperature
    val minW = if(other.validWindSpeed) minWindSpeed.min(other.windSpeed) else minWindSpeed
    val maxW = if(other.validWindSpeed) maxWindSpeed.max(other.windSpeed) else maxWindSpeed
    WeatherMinMax(minT,maxT,minW,maxW)
  }
}
