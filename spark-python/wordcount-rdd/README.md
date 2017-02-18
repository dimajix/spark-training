# PySpark WordCount

This is the trivial example for performing a word count with Spark. But in contrast to the wordcount-mini example
this example serves as a more complete application skeleton.
 
## Prerequistite

You need to have some text inside you HDFS home dircetory.

## Running

    spark-submit --master yarn-client wordcount.py --input=alice.txt --output=counts.txt
        