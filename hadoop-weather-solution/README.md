# Weather

This project contains an improved implementation Java Map/Reduce implementation of a simple weather analytics. It
 contains the following improvements:
 
 * Using a Combiner
 * Aggregating more Values (Wind)
 * Using of country lookup table

## Building

You can simply build the example with Maven via

        mvn install
        

## Running
        
The example also contains a small starter script. You can invoke the example via
        
        run.sh -i <inputDir> -o <outputDir>
        
Before running the example you have to upload some text into HDFS. In this example we simply upload the README.md
file itself:

        # Create source directory
        hdfs dfs -mkdir weather
        # Upload text file
        hdfs dfs -put ../../data/weather/2011 weather
        
        # Finally run example
        ./run.sh -i weather/2011 -o weather/minmax -c file:///home/cloudera/data/weather/ish-history.csv
        
        # Retrieve result
        hdfs dfs -cat weather/minmax

