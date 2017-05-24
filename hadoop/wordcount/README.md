# WordCount

This project contains an implementation of the famopus word count example. 

## Building

You can simply build the example with Maven via

        mvn install
        

## Running
        
The example also contains a small starter script. You can invoke the example via
        
        run.sh -i <inputDir> -o <outputDir>
        
Before running the example you have to upload some text into HDFS. In this example we simply upload the README.md
file itself:

        # Create source directory
        hdfs dfs -mkdir wordcount-input
        # Create output directory
        hdfs dfs -mkdir wordcount-output
        # Upload text file
        hdfs dfs -put README.md wordcount-input
        
        # Finally run example
        ./run.sh -i wordcount-input -o wordcount-output
        
        # Retrieve result
        hdfs dfs -getmerge wordcount-output wordcount-output.txt


        