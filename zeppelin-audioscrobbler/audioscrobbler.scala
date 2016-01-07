// Load User-Artist data
val rawUserArtistData = sc.textFile("/user/cloudera/data/audioscrobbler/user_artist_data.txt")
rawUserArtistData.take(5).foreach(println)

// Check if IDs match into 32bit range
rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
rawUserArtistData.map(_.split(' ')(1).toDouble).stats()

// Load Artist names
val rawArtistData = sc.textFile("/user/cloudera/data/audioscrobbler/artist_data.txt")
rawArtistData.take(5).foreach(println)

val artistsById = rawArtistData.flatMap { line =>
  val (id, name) = line.span(_ != '\t')
  if (name.isEmpty) {
    None
  }
  else {
    try {
      Some((id.toInt, name.trim))
    }
    catch {
      case e: NumberFormatException => None
    }
  }
}
artistsById.take(5).foreach(println)

// Load Artist Aliases
val rawArtistAlias = sc.textFile("/user/cloudera/data/audioscrobbler/artist_alias.txt")
rawArtistAlias.take(5).foreach(println)

val artistAlias = rawArtistAlias.flatMap { line =>
  val (alias, id) = line.span(_ != '\t')
  if (id.isEmpty) {
    None
  }
  else {
    try {
      Some((alias.toInt, id.trim.toInt))
    }
    catch {
      case e: NumberFormatException => None
    }
  }
}
artistAlias.take(5).foreach(println)

// Check if aliases make sense
artistAlias.join(artistsById).map(_._2).groupByKey.take(5).foreach(println)

// Create normalized version
val normalizedArtistById = artistsById
  .leftOuterJoin(artistAlias)
  .map(x => (x._2._2.getOrElse(x._1), x._2._1))
  .groupByKey()
  .mapValues(_.head)


// Now create userArtistData
import org.apache.spark.mllib.recommendation.Rating
val userArtistData = rawUserArtistData
  .repartition(32)
  .map(line => {
    val Array(userId,artistId,count) = line.split(' ').map(_.toInt)
    (artistId, (userId, count))
  })
  .leftOuterJoin(artistAlias)
  .map(record => {
    val userId = record._2._1._1
    val count = record._2._1._2
    val artistId = record._2._2.getOrElse(record._1)
    Rating(userId, artistId, count)
  })
  .cache()


// Look at some general statistics
println("Ratings per user: " + userArtistData.map(x => (x.user, 1.0)).reduceByKey(_ + _).map(_._2).stats.toString)
println("Ratings per artist: " + userArtistData.map(x => (x.product, 1.0)).reduceByKey(_ + _).map(_._2).stats.toString)

// Make a histogram of entries per user
val userCountHistogram = userArtistData.map(x => (x.user, 1.0)).reduceByKey(_ + _).map(_._2).histogram(100)
println("%table")
println("entries\tusers")
for (i <- 0 to userCountHistogram._1.length)
  println(userCountHistogram._1(i).toString + '\t' + userCountHistogram._2(i).toString)


// Make a histogram of entries per artist
val artistCountHistogram = userArtistData.map(x => (x.product, 1.0)).reduceByKey(_ + _).map(_._2).histogram(100)
println("%table")
println("entries\tartists")
for (i <- 0 to artistCountHistogram._1.length)
  println(artistCountHistogram._1(i).toString + '\t' + artistCountHistogram._2(i).toString)


// Now create a nice model
import org.apache.spark.mllib.recommendation.ALS
val rank = 10
val iterations = 5
val lambda = 0.01
val alpha = 1.0
val model = ALS.trainImplicit(userArtistData, rank, iterations, lambda, alpha)


// Lets examine the model
model.userFeatures.mapValues(_.mkString(", ")).take(5).foreach(println)
model.productFeatures.mapValues(_.mkString(", ")).take(5).foreach(println)


// Examine a single user
val products_2093760 = userArtistData.filter(_.user == 2093760).map(_.product).collect.toSet
artistsById.filter(x => products_2093760.contains(x._1)).map(_._2).collect.foreach(println)


// Find users with some reasonable amount of data
userArtistData.map(x => (x.user, 1))
    .reduceByKey(_ + _)
    .filter(_._2 < 500)
    .sortBy(_._2, ascending=false)
    .take(30)
    .foreach(println)

// Pick out a single user
userArtistData
  .filter(_.user == 2030067)
  .map(x => (x.product, 1))
  .reduceByKey(_ + _)
  .join(artistsById)
  .map(x => (x._2._2, x._2._1))
  .sortBy(_._2, ascending=false)
  .take(30)
  .foreach(println)


//*********************************************************************************************************************
//
// MODEL EVALUATION
//
//*********************************************************************************************************************

import java.util.Random
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

def genNegativeUserProducts(positiveData: RDD[Rating], allProducts: Array[Int]) = {
  // Create broadcast variable
  val bAllProducts = positiveData.sparkContext.broadcast(allProducts)
  // Take held-out data as the "positive", and map to tuples
  val positiveUserProducts = positiveData.map(r => (r.user, r.product))

  // Create a set of "negative" products for each user. These are randomly chosen
  // from among all of the other items, excluding those that are "positive" for the user.
  val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
    // mapPartitions operates on many (user,positive-items) pairs at once
    userAndItem => {
      // Init an RNG and the item IDs set once for partition
      val random = new Random()
      val allProducts = bAllProducts.value
      userAndItem.map { case (user, positiveItems) =>
        val positiveItemSet = positiveItems.toSet
        val negativeItems = new ArrayBuffer[Int]()
        // Keep about as many negative examples per user as positive.
        // Duplicates are OK
        while (negativeItems.size < positiveItemSet.size) {
          val item = allProducts(random.nextInt(allProducts.size))
          if (!positiveItemSet.contains(item)) {
            negativeItems += item
          }
        }
        // Result is a collection of (user,negative-item) tuples
        negativeItems.map(item => (user, item))
      }
    }
  }
  // flatMap breaks the collections above down into one big set of tuples
  .flatMap(t => t)
}


def evaluatePrediction(
                    positiveData: RDD[Rating],
                    allProducts: Array[Int],
                    predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
  // Take held-out data as the "positive", and map to tuples
  val positiveUserProducts = positiveData.map(r => (r.user, r.product))
  // Make predictions for each of them, including a numeric score, and gather by user
  val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

  // Create a set of "negative" products for each user. These are randomly chosen
  // from among all of the other items, excluding those that are "positive" for the user.
  val negativeUserProducts = genNegativeUserProducts(positiveData, allProducts)
  // Make predictions on the rest:
  val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

  // Join positive and negative by user
  positivePredictions.join(negativePredictions).values.map {
    case (positiveRatings, negativeRatings) =>
      // The value may be viewed as the probability that a random positive item scores
      // higher than a random negative one. Here the proportion of all positive-negative
      // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
      var correct = 0L
      var total = 0L
      // For each pairing,
      for (positive <- positiveRatings;
           negative <- negativeRatings) {
        // Count the correctly-ranked pairs
        if (positive.rating > negative.rating) {
          correct += 1
        }
        total += 1
      }
      // Return AUC: fraction of pairs ranked correctly
      correct.toDouble / total
  }.mean() // Return mean AUC over users
}


// Split data into training data and validation data
val Array(trainData, validationData) = userArtistData.randomSplit(Array(0.9, 0.1))
trainData.setName("Training Data").cache()
validationData.setName("Validation Data").cache()

// Create model from training data
val rank = 10
val iterations = 5
val lambda = 0.01
val alpha = 1.0
val model = ALS.trainImplicit(trainData, rank, iterations, lambda, alpha)

val allProducts = normalizedArtistById.map(_._1).collect()
evaluatePrediction(validationData, allProducts, model.predict)


// Predictor which simply returns the number of listenes
val mostListened = trainData.map(r => (r.product, r.rating)).reduceByKey(_ + _)
val bMostListened = sc.broadcast(mostListened.collectAsMap())
def predictMostListened(allData: RDD[(Int,Int)]) = {
  allData.map { case (user, product) =>
    Rating(user, product, bMostListened.value.getOrElse(product, 0.0))
  }
}

evaluatePrediction(validationData, allProducts, predictMostListened)


//*********************************************************************************************************************
//
// FREQUENT vs INFREQUENT ARTISTS PREDICTION
//
//*********************************************************************************************************************

val frequentUserArtistData = userArtistData
  .map(x => (x.product, x))
  .join(frequentArtists)
  .map(_._2._1)

val infrequentUserArtistData = userArtistData
  .map(x => (x.product, x))
  .join(infrequentArtists)
  .map(_._2._1)

frequentUserArtistData.count
infrequentUserArtistData.count

case class ALSParams(rank:Int = 10, iterations:Int = 5, lambda:Double = 0.01, alpha:Double = 1.0) { }
def trainAndCompare(trainData:RDD[Rating], validationData:RDD[Rating], params: ALSParams = ALSParams()) = {
  // Extract list of all products from ratings
  val allProducts = ratings.map(_.product).distinct.collect

  // Create and evaluate ALS model
  val model = ALS.trainImplicit(trainData, params.rank, params.iterations, params.lambda, params.alpha)
  val alsScore = evaluatePrediction(validationData, allProducts, model.predict)

  // Create and evaluate simplistic model
  val mostListened = sc.broadcast(trainData.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
  def predictMostListened(allData: RDD[(Int,Int)]) = {
    allData.map { case (user, product) =>
      Rating(user, product, mostListened.value.getOrElse(product, 0.0))
    }
  }
  val simpleScore = evaluatePrediction(validationData, allProducts, predictMostListened)

  println("ALS method with " + params.toString + ": " + alsScore.toString)
  println("Simple method: " + simpleScore.toString)
}

def splitTrainAndCompare(ratings:RDD[Rating], params: ALSParams = ALSParams()) = {
  // Split whole data intro training and validation data
  val Array(trainData, validationData) = ratings.randomSplit(Array(0.9, 0.1))
  trainData.setName("Training Data").cache()
  validationData.setName("Validation Data").cache()

  trainAndCompare(trainData, validationData, params)

  // Clean up
  trainData.unpersist()
  validationData.unpersist()
}


//*********************************************************************************************************************
//
// HYPERPARAMETER SELECTIION
//
//*********************************************************************************************************************

val Array(trainData, validationData) = ratings.randomSplit(Array(0.9, 0.1))
trainData.setName("Training Data").cache()
validationData.setName("Validation Data").cache()

for (rank <- Array(10,20,50);
     iterations <- Array(10,20,50);
     lambda <- Array(0.001, 0.1, 1.0);
     alpha <- Array(1.0, 10.0, 40.0)) {

  val params = ALSParams(rank, iterations, lambda, alpha)
  trainAndCompare(trainData, validationData, params)
}

trainData.unpersist()
validationData.unpersist()

