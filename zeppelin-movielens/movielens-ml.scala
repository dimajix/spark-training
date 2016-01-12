case class MovieLink(movie:Int,imdb:Int,tmdb:Int) { }
def extractLink(line:String) = {
  val fields = line.split(',')
  if (fields(0) != "movieId")
    Some(MovieLink(fields(0).toInt,fields(1).toInt,fields(2).toInt))
  else
    None
}
val links = sc.textFile("/user/cloudera/data/movielens/links.csv")
  .flatMap(extractLink)
  .toDF
links.take(5).foreach(println)


case class MovieRating(user:Int,movie:Int,rating:Float,ts:Int) { }
def extractRating(line:String) = {
  val fields = line.split(',')
  if (fields(0) != "userId")
    Some(MovieRating(fields(0).toInt,fields(1).toInt,fields(2).toFloat,fields(3).toInt))
  else
    None
}
val ratings = sc.textFile("/user/cloudera/data/movielens/ratings.csv")
  .flatMap(extractRating)
  .toDF
ratings.take(5).foreach(println)


case class MovieTag(user:Int,movie:Int,tag:String,ts:Int)
def extractTag(line:String) = {
  val fields = line.split(',')
  if (fields(0) != "userId")
    Some(MovieTag(fields(0).toInt,fields(1).toInt,fields(2),fields(3).toInt))
  else
    None
}
val tags = sc.textFile("/user/cloudera/data/movielens/tags.csv")
  .flatMap(extractTag)
  .toDF
tags.take(5).foreach(println)


case class Movie(movie:Int,title:String,genres:Seq[String])
def extractMovie(line:String) = {
  val fields = line.split(',')
  if (fields(0) != "movieId")
    Some(Movie(fields(0).toInt,fields(1),fields(2).split('|').toSeq))
  else
    None
}
val movies = sc.textFile("/user/cloudera/data/movielens/movies.csv")
  .flatMap(extractMovie)
  .toDF
movies.take(5).foreach(println)


println("Ratings per user: " + ratings.groupBy("user").count().rdd.map(_.getLong(1).toDouble).stats)
println("Ratings per movie: " + ratings.groupBy("movie").count().rdd.map(_.getLong(1).toDouble).stats)


// Make a histogram of entries per user
val userCountHistogram = ratings.groupBy("user").count().rdd.map(_.getLong(1).toDouble).histogram(100)
println("%table")
println("ratings\tusers")
for (i <- 0 to userCountHistogram._1.length)
  println(userCountHistogram._1(i).toString + '\t' + userCountHistogram._2(i).toString)


// Make a histogram of entries per artist
val movieCountHistogram = ratings.groupBy("movie").count().rdd.map(_.getLong(1).toDouble).histogram(100)
println("%table")
println("ratings\tmovies")
for (i <- 0 to movieCountHistogram._1.length)
  println(movieCountHistogram._1(i).toString + '\t' + movieCountHistogram._2(i).toString)




// Split data into training data and validation data
val splitData = ratings.rdd.randomSplit(Array(0.9, 0.1))
val trainData = sqlContext.createDataFrame(splitData(0), ratings.schema).cache()
val validationData = sqlContext.createDataFrame(splitData(1), ratings.schema).cache()

import org.apache.spark.ml.recommendation.ALS
val als = new ALS()
  .setRank(20)
  .setMaxIter(10)
  .setImplicitPrefs(false)
  .setItemCol("movie")
  .setUserCol("user")
  .setRatingCol("rating")
  .setPredictionCol("prediction")

val model = als.fit(trainData)


import org.apache.spark.sql.DataFrame
def evaluatePrediction(
      validationData: DataFrame,
      predictFunction: (DataFrame => DataFrame) )= {
  val predictedRatings = predictFunction(validationData)
    .select("user","movie","prediction")
  val mse = validationData.join(predictedRatings, Seq("user","movie"))
    .select("rating", "prediction")
    .rdd
    .flatMap { row =>
      val err = (row.getFloat(0) - row.getFloat(1))
      val err2 = err * err
      if (err2.isNaN) {
        None
      } else {
        Some(err2)
      }
    }.mean()
  scala.math.sqrt(mse)
}

def avgUserRating(trainData:DataFrame)(userMovies:DataFrame) = {
  val avgPerUser = trainData.groupBy("user")
    .agg(avg("rating") as "avg_rating"  cast org.apache.spark.sql.types.FloatType)
  userMovies.join(avgPerUser, "user")
    .withColumn("prediction", $"avg_rating")
    .select("user", "movie", "prediction")
}

def avgMovieRating(trainData:DataFrame)(userMovies:DataFrame) = {
  val avgPerMovie = trainData.groupBy("movie")
    .agg(avg("rating") as "avg_rating"  cast org.apache.spark.sql.types.FloatType)
  userMovies.join(avgPerMovie, "movie")
    .withColumn("prediction", $"avg_rating")
    .select("user", "movie", "prediction")
}



evaluatePrediction(validationData, model.predict)
evaluatePrediction(validationData, avgUserRating(trainData))
evaluatePrediction(validationData, avgMovieRating(trainData))
