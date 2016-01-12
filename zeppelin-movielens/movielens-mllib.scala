%dep
z.load("com.opencsv:opencsv:3.6")

import org.apache.spark.storage.StorageLevel

case class MovieLink(movie:Int,imdb:Int,tmdb:Int) { }
def extractLink(line:String) = {
  val fields = line.split(',')
  if (fields(0) != "movieId" && fields.size >= 3)
    Some(MovieLink(fields(0).toInt,fields(1).toInt,fields(2).toInt))
  else
    None
}
val links = sc.textFile("/user/cloudera/data/movielens/links.csv")
  .flatMap(extractLink)
  .setName("Movie Links")
  .persist(StorageLevel.MEMORY_AND_DISK)
links.take(5).foreach(println)


case class MovieRating(user:Int,movie:Int,rating:Float,ts:Int) { }
def extractRating(line:String) = {
  val fields = line.split(',')
  if (fields(0) != "userId" && fields.size >= 4)
    Some(MovieRating(fields(0).toInt,fields(1).toInt,fields(2).toFloat,fields(3).toInt))
  else
    None
}
val ratings = sc.textFile("/user/cloudera/data/movielens/ratings.csv")
  .flatMap(extractRating)
  .repartition(16)
  .setName("Movie Ratings")
  .persist(StorageLevel.MEMORY_AND_DISK)
ratings.take(5).foreach(println)


case class MovieTag(user:Int,movie:Int,tag:String,ts:Int)
def extractTag(line:String) = {
  val parser = new com.opencsv.CSVParser(',','\"')
  val fields = parser.parseLine(line)
  if (fields(0) != "userId" && fields.size >= 4)
    Some(MovieTag(fields(0).toInt,fields(1).toInt,fields(2),fields(3).toInt))
  else
    None
}
val tags = sc.textFile("/user/cloudera/data/movielens/tags.csv")
  .flatMap(extractTag)
  .setName("Movie Tags")
  .persist(StorageLevel.MEMORY_AND_DISK)
tags.take(5).foreach(println)


case class Movie(movie:Int,title:String,genres:Seq[String])
def extractMovie(line:String) = {
  val parser = new com.opencsv.CSVParser(',','\"')
  val fields = parser.parseLine(line)
  if (fields(0) != "movieId" && fields.size >= 3)
    Some(Movie(fields(0).toInt,fields(1),fields(2).split('|').toSeq))
  else
    None
}
val movies = sc.textFile("/user/cloudera/data/movielens/movies.csv")
  .flatMap(extractMovie)
  .setName("Movies")
  .persist(StorageLevel.MEMORY_AND_DISK)
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
val movieCountHistogram = ratings.map(x => (x.movie, 1.0)).reduceByKey(_ + _).map(_._2).histogram(100)
println("%table")
println("ratings\tmovies")
for (i <- 0 to movieCountHistogram._1.length)
  println(movieCountHistogram._1(i).toString + '\t' + movieCountHistogram._2(i).toString)



//*********************************************************************************************************************
//
// MODEL CONSTRUCTION
//
//*********************************************************************************************************************

// Build Model
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

val mlRatings = ratings.map(x => Rating(x.user, x.movie, x.rating))

val rank = 10
val iterations = 5
val lambda = 0.01
val model = ALS.train(mlRatings, rank, iterations, lambda)


//*********************************************************************************************************************
//
// MODEL EXPLORATION
//
//*********************************************************************************************************************

val moviesById = movies.map(x => (x.movie,x)).collect.toMap

// Look at movies of user 1
// Other good users: 100, 200
ratings.filter(_.user == 1).collect.map(x => (moviesById(x.movie).title, x.rating)).sortBy(-_._2).foreach(println)
model.recommendProducts(1,10).map(x => (moviesById(x.product).title, x.rating)).foreach(println)


// FInd Wes Cravens Nightmare (should be 366)
movies.filter(_.title.contains("Nightmare")).collect.foreach(println)

// Get Feature Vector of model for movie
val features_366 = model.productFeatures.lookup(366).head

def cosineSimilarity(feature1:Array[Double], feature2:Array[Double]) = {
  var dotSum = 0.0
  var sqSum1 = 0.0
  var sqSum2 = 0.0
  var i = 0
  for (i <- 0 to feature1.size) {
    val f1 = feature1(i)
    val f2 = feature2(i)
    dotSum += f1*f2
    sqSum1 += f1*f1
    sqSum2 += f2*f2
  }
  dotSum / (scala.math.sqrt(sqSum1) * scala.math.sqrt(sqSum2))
}

// Find similar movies
val similarities = model.productFeatures.map(x => (x._1, cosineSimilarity(x._2, feature_366)))
similarities.top(10)(Ordering.by { case (id,similarity) => similarity})
  .map { case (id,similarity) => (moviesById(id), similarity) }
  .foreach(println)


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


// Definition of evaluation function
import org.apache.spark.rdd.RDD
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


// Alternative definition of evaluation function
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.RegressionMetrics
def evaluatePrediction(
                        validationData: RDD[Rating],
                        predictFunction: (RDD[(Int,Int)] => RDD[Rating]) ) = {
  val predictedRatings = predictFunction(validationData.map(x => (x.user, x.product)))
  val ratingsAndPredictions = validationData.map(x => ((x.user, x.product), x.rating))
    .join(
      predictedRatings.map(x => ((x.user, x.product), x.rating))
    )
    .values
  val metrics = new RegressionMetrics(ratingsAndPredictions)
  metrics.rootMeanSquaredError
}

// Use average rating of the users as prediction
def avgUserRating(ratings:RDD[Rating])(userProducts:RDD[(Int,Int)]) = {
  val avgPerUser = ratings
    .map(x => (x.user, x.rating))
    .groupByKey()
    .mapValues(x => x.toSeq.sum / x.size)
  userProducts.join(avgPerUser)
    .map { case (user,(product,rating)) => Rating(user,product,rating)}
}

// Use average rating of the product as prediction
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



//*********************************************************************************************************************
//
// HYPER PARAMETER TUNING
//
//*********************************************************************************************************************

// Split data into training data and validation data
//val mlRatings = ratings.map(x => Rating(x.user, x.movie, x.rating))
//val Array(trainData, validationData) = mlRatings.randomSplit(Array(0.9, 0.1))
//trainData.setName("Training Data").cache()
//validationData.setName("Validation Data").cache()

val rankScores = (3 to 15).map { f =>
  val rank = f
  val iterations = 10
  val lambda = 0.1
  val model = ALS.train(trainData, rank, iterations, lambda)

  val score = evaluatePrediction(validationData, model.predict)
  (f,score)
}

println("%table")
println("rank\tscore")
rankScores.foreach(x => println(x._1 + "\t" + x._2))



//*********************************************************************************************************************
//
// MOVIE CLUSTERING
//
//*********************************************************************************************************************

import org.apache.spark.mllib.linalg.Vectors
val movieFactors = model.productFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
val movieVectors = movieFactors.map(_._2)

import org.apache.spark.mllib.linalg.distributed.RowMatrix
val movieMatrix = new RowMatrix(movieVectors)
val movieStats = movieMatrix.computeColumnSummaryStatistics()
println("Movie factores mean: " + movieStats.mean)
println("Movie factores variance: " + movieStats.variance)

import org.apache.spark.mllib.clustering.KMeans
val numClusters = 10
val numIterations = 20
val numRuns = 5
val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterations, numRuns)

// Squared Distance function
import org.apache.spark.mllib.linalg.DenseVector
def calcDistance(v1:DenseVector, v2:DenseVector) = {
  import breeze.linalg._
  import breeze.numerics.pow
  scala.math.sqrt(pow(v1.values - v2.values, 2).sum)
}

// Make Cluster Prediction for every Movie
movieFactors.map { case (movie, factors) =>
    val pred = movieClusterModel.predict(factors)
    val center = movieClusterModel.clusterCenters(pred)
    val dist = calcDistance(factors.toDense, center.toDense)
    (movie, (pred, dist))
  }
  // Join with Movie database
  .join(movies.map(x => (x.movie, x.title)))
  // Extract relevant information
  .map { case (movie, ((cluster, distance), title)) => (cluster, (title, distance)) }
  // Put movies together into their clusters
  .groupByKey()
  // Sort each cluster by distance, and take 10 Examples
  .mapValues(_.toSeq.sortBy(_._2).take(10))
  // Retrieve Result
  .collect
  // Print result
  .foreach { case (cluster, movies) => movies.foreach(m => println(cluster.toString + ": " + m._1 + " " + m._2)) }

