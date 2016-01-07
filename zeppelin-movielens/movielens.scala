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
val moviesById = movies.map(x => (x.movie,x)).collect.toMap
movies.take(5).foreach(println)


// Look at some statistics
println("Ratings per user: " + ratings.map(x => (x.user, 1.0)).reduceByKey(_ + _).map(_._2).stats.toString)
println("Ratings per movie: " + ratings.map(x => (x.movie, 1.0)).reduceByKey(_ + _).map(_._2).stats.toString)


// Make a histogram of entries per user
val userCountHistogram = ratings.map(x => (x.user, 1.0)).reduceByKey(_ + _).map(_._2).histogram(100)
println("%table")
println("ratings\tusers")
for (i <- 0 to userCountHistogram._1.length)
  println(userCountHistogram._1(i).toString + '\t' + userCountHistogram._2(i).toString)


// Make a histogram of entries per artist
val artistCountHistogram = ratings.map(x => (x.movie, 1.0)).reduceByKey(_ + _).map(_._2).histogram(100)
println("%table")
println("ratings\tmovies")
for (i <- 0 to artistCountHistogram._1.length)
  println(artistCountHistogram._1(i).toString + '\t' + artistCountHistogram._2(i).toString)


// Build Model
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

val mlRatings = ratings.map(x => Rating(x.user, x.movie, x.rating))

val rank = 10
val iterations = 5
val lambda = 0.01
val model = ALS.train(mlRatings, rank, iterations, lambda)

// Look at movies of user 1
val moviesById = movies.map(x => (x.movie,x)).collect.toMap
ratings.filter(_.user == 1).collect.map(x => (moviesById(x.movie).title, x.rating)).sortBy(-_._2).foreach(println)

model.recommendProducts(1,10).map(x => (moviesById(x.product).title, x.rating)).foreach(println)



//*********************************************************************************************************************
//
// MODEL EVALUATION
//
//*********************************************************************************************************************

// Split data into training data and validation data
val mlRatings = ratings.map(x => Rating(x.user, x.movie, x.rating))
val Array(trainData, validationData) = mlRatings.randomSplit(Array(0.9, 0.1))
trainData.setName("Training Data").cache()
validationData.setName("Validation Data").cache()

val rank = 20
val iterations = 10
val lambda = 0.1
val model = ALS.train(mlRatings, rank, iterations, lambda)


def evaluatePrediction(
    validationData: RDD[Rating],
    predictFunction: (RDD[(Int,Int)] => RDD[Rating]) )= {
  val predictedRatings = predictFunction(validationData.map(x => (x.user, x.product)))
  val ratingsAndPredictions = validationData.map(x => ((x.user, x.product), x.rating))
    .join(
      predictedRatings.map(x => ((x.user, x.product), x.rating))
    )
  val variance = ratingsAndPredictions.map { case ((user, product), (r1, r2)) =>
    val err = (r1 - r2)
    err * err
  }.mean()
  scala.math.sqrt(variance)
}


def avgUserRating(ratings:RDD[Rating])(userProducts:RDD[(Int,Int)]) = {
  val avgPerUser = ratings
    .map(x => (x.user, x.rating))
    .groupByKey()
    .mapValues(x => x.toSeq.sum / x.size)
  userProducts.join(avgPerUser)
    .map { case (user,(product,rating)) => Rating(user,product,rating)}
}

def avgProductRating(ratings:RDD[Rating])(userProducts:RDD[(Int,Int)]) = {
  val avgPerProduct = ratings
    .map(x => (x.product, x.rating))
    .groupByKey()
    .mapValues(x => x.toSeq.sum / x.size)
  userProducts.map(x => (x._2,x._1))
    .join(avgPerProduct)
    .map { case (product,(user,rating)) => Rating(user,product,rating)}
}


evaluatePrediction(validationData, model.predict)
evaluatePrediction(validationData, avgUserRating(trainData))
evaluatePrediction(validationData, avgProductRating(trainData))
