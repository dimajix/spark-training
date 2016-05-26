# PySpark WordCount

This is the trivial example for performing a word count with Spark.
 
## Prerequistite

You need to have a text called 'alice.txt' inside you HDFS home dircetory.

## Running

    spark-submit --master yarn-client wordcount.py
    
## Output
    
The program will create a new directory 'alice_wordcount' inside your HDFS home directory.
    