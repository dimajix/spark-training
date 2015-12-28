// First look into data
val weather_2011 = sc.textFile("/user/cloudera/weather/2011")
weather_2011.take(10).foreach(println)

// Count data from all years
var totals:Long = 0
for(i <- 2005 to 2011) {
  val wy = sc.textFile("/user/cloudera/weather/" + i.toString)
  totals = totals + wy.count()
}
totals

// Do it in a more elegant way
(2005 to 2011) map {i => sc.textFile("/user/cloudera/weather/" + i.toString).count()} reduce(_ + _)



case class WeatherData(
    date:String,
    time:String,
    usaf:String,
    wban:String,
    validTemperature:Boolean,
    temperature:Float,
    validWindSpeed:Boolean,
    windSpeed:Float
)
def extractWeatherData(row:String) = {
  val date = row.substring(15,23)
  val time = row.substring(23,27)
  val usaf = row.substring(4,10)
  val wban = row.substring(10,15)
  val airTemperatureQuality = row.charAt(92)
  val airTemperature = row.substring(87,92)
  val windSpeedQuality = row.charAt(69)
  val windSpeed = row.substring(65,69)

  WeatherData(date,time,usaf,wban,airTemperatureQuality == '1',airTemperature.toFloat/10,windSpeedQuality == '1',windSpeed.toFloat/10)
}

val raw_weather_years = (2005 to 2011) map {i => sc.textFile("/user/cloudera/weather/" + i.toString)}
val raw_weather = sc.union(raw_weather_years)
val weather = raw_weather.map(extractWeatherData(_))


case class StationData(
    usaf:String,
    wban:String,
    name:String,
    country:String,
    fips:String,
    state:String,
    call:String,
    latitude:Int,
    longitude:Int,
    elevation:Int,
    date_begin:String,
    date_end:String
)

def getInt(str:String) : Integer = {
  if (str.isEmpty)
    return 0
  else
    return str.toInt
}
def extractStationData(row:String) = {
  val columns = row.split(",").map(_.replaceAll("\"",""))
  val latitude = getInt(columns(7))
  val longitude = getInt(columns(8))
  val elevation = getInt(columns(9))
  StationData(columns(0),columns(1),columns(2),columns(3),columns(4),columns(5),columns(6),latitude,longitude,elevation,columns(10),columns(11))
}

val ish_raw = sc.textFile("/user/cloudera/weather/ish")
val ish_head = ish_raw.first
val ish = ish_raw
    .filter(_ != ish_head)
    .map(extractStationData)

val weather_idx = weather.keyBy(x => x.usaf + x.wban)
val ish_idx = ish.keyBy(x => x.usaf + x.wban)

val weather_per_country_and_year = weather_idx
    .join(ish_idx)
    .map(x =>
        (
          (x._2._2.country,x._2._1.date.substring(0,4)),
          x._2._1
        )
    )
weather_per_country_and_year.take(10).foreach(println)


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
val weather_minmax = weather_per_country_and_year.aggregateByKey(WeatherMinMax())((x:WeatherMinMax,y:WeatherData)=>x.reduce(y),(x:WeatherMinMax,y:WeatherMinMax)=>x.reduce(y))

weather_minmax.collect().foreach(println)


def mkString(p:Any*) = {
  p.iterator.toList.mkString("\t")
}

println("%table")
println("Country\tYear\tTempMin\tTempMax\tWindMin\tWindMax")
weather_minmax.collect().map(x => mkString(x._1._1,x._1._2,x._2.minTemperature,x._2.maxTemperature,x._2.minWindSpeed,x._2.maxWindSpeed)).foreach(println)
