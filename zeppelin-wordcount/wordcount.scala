val data = sc.textFile("alice")

// Look inside
data.take(10).foreach(println)

// Look inside
data.count()

val sample = data.take(10)
sample.foreach(println)

// Split text into words
val words = data.flatMap(_.split(" "))
words.take(10).foreach(println)

// Count words
val wc = words.countByValue() //.toArray.sortBy(x => -x._2)
println("%table")
println("word\tcount")
wc.foreach(x => println(x._1 + "\t" + x._2))

// Full solution
val wc = words.filter(_ != "")
    .map(x => (x,1))
    .reduceByKey(_ + _)
    .sortBy(_._2,ascending=false)
println("%table")
println("word\tcount")
wc.take(10).foreach(x => println(x._1 + "\t" + x._2))
