// Load Data
val data = sc.textFile("/user/cloudera/data/bike-sharing/hour_nohead.csv")
  .map(_.split(','))

// Have a look at the data
data.take(3).foreach(x => println(x.mkString(",")))

// Define a class for modeling the data
case class BikeSharingData(
                            date:String,
                            season:Int,
                            year:Int,
                            month:Int,
                            hour:Int,
                            holiday:Int,
                            weekday:Int,
                            workingday:Int,
                            weather:Int,
                            temperature:Double,
                            apparentTemperature:Double,
                            humidity:Double,
                            windSpeed:Double,
                            count:Int) {
}

def extractData(line:String) = {
  val x = line.split(',')
  new BikeSharingData(
    x(1), // date
    x(2).toInt, // season
    x(3).toInt, // year
    x(4).toInt, // month
    x(5).toInt, // hour
    x(6).toInt, // holiday
    x(7).toInt, // weekday
    x(8).toInt, // workingday
    x(9).toInt, // weathersituation
    x(10).toDouble, // temperature
    x(11).toDouble, // apparent temperature
    x(12).toDouble, // humidty
    x(13).toDouble, // wind speed
    x(14).toInt // count
  )
}

// Load data again, now transform into a data set
val data = sc.textFile("/user/cloudera/data/bike-sharing/hour_nohead.csv")
  .map(extractData).toDF

// Transform some columns into doubles, otherwise ML won't work
import org.apache.spark.ml.feature.OneHotEncoder
val ddata = data.withColumn("season", $"season".cast("Double"))
  .withColumn("year", $"year".cast("Double"))
  .withColumn("month", $"month".cast("Double"))
  .withColumn("hour", $"hour".cast("Double"))
  .withColumn("holiday", $"holiday".cast("Double"))
  .withColumn("weekday", $"weekday".cast("Double"))
  .withColumn("workingday", $"workingday".cast("Double"))
  .withColumn("weather", $"weather".cast("Double"))
  .withColumn("count", $"count".cast("Double"))
val Array(trainData,testData) = ddata.randomSplit(Array(0.9,0.1))



//---------- Make nice picture over time ------------------------------------
println("%table")
println("date\tcount")
ddata.withColumn("day", unix_timestamp($"date", "yyyy-MM-dd"))
  .select("day","count")
  .collect()
  .foreach(x => println(x.getLong(0).toString + "\t" + x.getDouble(1).toString))


//---------- Calc variance ------------------------------------
ddata.agg(avg($"count")).collect.foreach(println)
ddata.agg(avg($"count"*$"count") - avg($"count")*avg($"count")).collect.foreach(println)


// Transform categorial parameters to dummy variables
println(new OneHotEncoder().explainParams())

val oneHotData = new OneHotEncoder()
  .setInputCol("season")
  .setOutputCol("hseason")
  .transform(data)


//---------- Regression with LinearRegression ------------------------------------
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel

println(new VectorAssembler().explainParams())
println(new LinearRegression().explainParams())

val pipe = new Pipeline()
  .setStages(Array(
    new StringIndexer()
      .setInputCol("season")
      .setOutputCol("iseason"),
    new OneHotEncoder()
      .setInputCol("iseason")
      .setOutputCol("vseason"),
    new StringIndexer()
      .setInputCol("year")
      .setOutputCol("iyear"),
    new OneHotEncoder()
      .setInputCol("iyear")
      .setOutputCol("vyear"),
    new StringIndexer()
      .setInputCol("hour")
      .setOutputCol("ihour"),
    new OneHotEncoder()
      .setInputCol("ihour")
      .setOutputCol("vhour"),
    new StringIndexer()
      .setInputCol("month")
      .setOutputCol("imonth"),
    new OneHotEncoder()
      .setInputCol("imonth")
      .setOutputCol("vmonth"),
    new StringIndexer()
      .setInputCol("holiday")
      .setOutputCol("iholiday"),
    new OneHotEncoder()
      .setInputCol("iholiday")
      .setOutputCol("vholiday"),
    new StringIndexer()
      .setInputCol("weekday")
      .setOutputCol("iweekday"),
    new OneHotEncoder()
      .setInputCol("iweekday")
      .setOutputCol("vweekday"),
    new StringIndexer()
      .setInputCol("workingday")
      .setOutputCol("iworkingday"),
    new OneHotEncoder()
      .setInputCol("iworkingday")
      .setOutputCol("vworkingday"),
    new StringIndexer()
      .setInputCol("weather")
      .setOutputCol("iweather"),
    new OneHotEncoder()
      .setInputCol("iweather")
      .setOutputCol("vweather"),
    new VectorAssembler()
      .setInputCols(Array("vseason", "vyear", "vmonth", "vhour", "vholiday", "vweekday", "vworkingday", "vweather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("count")
  ))
val pmodel = pipe.fit(trainData)
val predictions = pmodel.transform(testData)


import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)

pmodel.stages(8).asInstanceOf[LinearRegressionModel].weights
pmodel.stages(8).asInstanceOf[LinearRegressionModel].summary.r2


//---------- Regression with DecisionTreeRegressor ------------------------------------
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor

println(new DecisionTreeRegressor().explainParams())

val pipe = new Pipeline()
  .setStages(Array(
    new VectorAssembler()
      .setInputCols(Array("season", "year", "month", "hour", "holiday", "weekday", "workingday", "weather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("count")
      .setMaxDepth(10)
  ))
val pmodel = pipe.fit(trainData)
val predictions = pmodel.transform(testData)


import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)



//---------- Regression with GBTRegressor ------------------------------------
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor

println(new GBTRegressor().explainParams())

val pipe = new Pipeline()
  .setStages(Array(
    new VectorAssembler()
      .setInputCols(Array("season", "year", "month", "hour", "holiday", "weekday", "workingday", "weather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new GBTRegressor()
      .setFeaturesCol("features")
      .setLabelCol("count")
      .setMaxDepth(10)
  ))
val pmodel = pipe.fit(trainData)
val predictions = pmodel.transform(testData)


import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)


//---------- Regression with RandomForestRegressor ------------------------------------
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor

println(new RandomForestRegressor().explainParams())

val pipe = new Pipeline()
  .setStages(Array(
    new VectorAssembler()
      .setInputCols(Array("season", "year", "month", "hour", "holiday", "weekday", "workingday", "weather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("count")
      .setMaxDepth(10)
  ))
val pmodel = pipe.fit(trainData)
val predictions = pmodel.transform(testData)


import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)



//---------- Make plot over time ------------------------------------
println("%table")
println("date\tcount")
ddata.withColumn("day", unix_timestamp($"date", "yyyy-MM-dd"))
  .select("day","count")
  .collect()
  .foreach(x => println(x.getLong(0).toString + "\t" + x.getDouble(1).toString))


//---------- Make histogram of counts ------------------------------------
val histogram = ddata.select($"count").rdd
  .map(_.getDouble(0))
  .histogram(100)
println("%table")
println("count\tfreq")
histogram._1.zip(histogram._2).foreach(x => println(x._1.toString + "\t" + x._2))


val histogram = ddata.select($"count").rdd
  .map(_.getDouble(0))
  .histogram(100)
println("%table")
println("count\tfreq")
histogram._1.zip(histogram._2).foreach(x => println(x._1.toString + "\t" + x._2))


//----------- Mean Squared Log Error -------------------------------------
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

def rmsle(df:DataFrame, lcol:String, pcol:String) = {
  scala.math.sqrt(
    df.select(col(lcol),col(pcol))
      .map{case Row(prediction: Double, label: Double) => scala.math.pow(scala.math.log(prediction+1) - scala.math.log(label + 1), 2.0) }
      .mean()
  )
}


//---------- Transform target variable ------------------------------------
val ltrainData = trainData.withColumn("log_count", log($"count" + 1.0))
val ltestData = testData.withColumn("log_count", log($"count" + 1.0))


//---------- RandomForestRegressor ------------------------------------
val pipe = new Pipeline()
  .setStages(Array(
    new VectorAssembler()
      .setInputCols(Array("season", "year", month", "hour", "holiday", "weekday", "workingday", "weather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("log_count")
      .setPredictionCol("log_prediction")
      .setMaxDepth(10)
  ))
val pmodel = pipe.fit(ltrainData)
val predictions = pmodel.transform(ltestData).withColumn("prediction", exp($"log_prediction") - 1.0)

import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)



//---------- LinearRegression ------------------------------------
val pipe = new Pipeline()
  .setStages(Array(
    new StringIndexer()
      .setInputCol("season")
      .setOutputCol("iseason"),
    new OneHotEncoder()
      .setInputCol("iseason")
      .setOutputCol("vseason"),
    new StringIndexer()
      .setInputCol("year")
      .setOutputCol("iyear"),
    new OneHotEncoder()
      .setInputCol("iyear")
      .setOutputCol("vyear"),
    new StringIndexer()
      .setInputCol("hour")
      .setOutputCol("ihour"),
    new OneHotEncoder()
      .setInputCol("ihour")
      .setOutputCol("vhour"),
    new StringIndexer()
      .setInputCol("month")
      .setOutputCol("imonth"),
    new OneHotEncoder()
      .setInputCol("imonth")
      .setOutputCol("vmonth"),
    new StringIndexer()
      .setInputCol("holiday")
      .setOutputCol("iholiday"),
    new OneHotEncoder()
      .setInputCol("iholiday")
      .setOutputCol("vholiday"),
    new StringIndexer()
      .setInputCol("weekday")
      .setOutputCol("iweekday"),
    new OneHotEncoder()
      .setInputCol("iweekday")
      .setOutputCol("vweekday"),
    new StringIndexer()
      .setInputCol("workingday")
      .setOutputCol("iworkingday"),
    new OneHotEncoder()
      .setInputCol("iworkingday")
      .setOutputCol("vworkingday"),
    new StringIndexer()
      .setInputCol("weather")
      .setOutputCol("iweather"),
    new OneHotEncoder()
      .setInputCol("iweather")
      .setOutputCol("vweather"),
    new VectorAssembler()
      .setInputCols(Array("vseason", "vyear", "vmonth", "vhour", "vholiday", "vweekday", "vworkingday", "vweather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("log_count")
      .setPredictionCol("log_prediction")
  ))
val pmodel = pipe.fit(ltrainData)
val predictions = pmodel.transform(ltestData).withColumn("prediction", exp($"log_prediction") - 1.0)

import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)

rmsle(predictions, "count", "prediction")


//---------- New Features ------------------------------------
val ext_data = ddata.withColumn("workday_hour", concat(col("workingday").cast("String"),lit("_"),col("hour").cast("String")))

val Array(ext_trainData,ext_testData) = ext_data.randomSplit(Array(0.9,0.1))
val ltrainData = ext_trainData.withColumn("log_count", log($"count" + 1.0))
val ltestData = ext_testData.withColumn("log_count", log($"count" + 1.0))


val pipe = new Pipeline()
  .setStages(Array(
    new StringIndexer()
      .setInputCol("season")
      .setOutputCol("iseason"),
    new OneHotEncoder()
      .setInputCol("iseason")
      .setOutputCol("vseason"),
    new StringIndexer()
      .setInputCol("year")
      .setOutputCol("iyear"),
    new OneHotEncoder()
      .setInputCol("iyear")
      .setOutputCol("vyear"),
    new StringIndexer()
      .setInputCol("month")
      .setOutputCol("imonth"),
    new OneHotEncoder()
      .setInputCol("imonth")
      .setOutputCol("vmonth"),
    new StringIndexer()
      .setInputCol("holiday")
      .setOutputCol("iholiday"),
    new OneHotEncoder()
      .setInputCol("iholiday")
      .setOutputCol("vholiday"),
    new StringIndexer()
      .setInputCol("weekday")
      .setOutputCol("iweekday"),
    new OneHotEncoder()
      .setInputCol("iweekday")
      .setOutputCol("vweekday"),
    new StringIndexer()
      .setInputCol("workday_hour")
      .setOutputCol("iworkingday"),
    new OneHotEncoder()
      .setInputCol("iworkingday")
      .setOutputCol("vworkingday"),
    new StringIndexer()
      .setInputCol("weather")
      .setOutputCol("iweather"),
    new OneHotEncoder()
      .setInputCol("iweather")
      .setOutputCol("vweather"),
    new VectorAssembler()
      .setInputCols(Array("vseason", "vyear", "vmonth", "vhour", "vholiday", "vweekday", "vworkingday", "vweather", "temperature", "apparentTemperature", "humidity", "windSpeed"))
      .setOutputCol("features"),
    new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("log_count")
      .setPredictionCol("log_prediction")
  ))
val pmodel = pipe.fit(ltrainData)
val predictions = pmodel.transform(ltestData).withColumn("prediction", exp($"log_prediction") - 1.0)

import org.apache.spark.ml.evaluation.RegressionEvaluator
new RegressionEvaluator()
  .setLabelCol("count")
  .setPredictionCol("prediction")
  .evaluate(predictions)
