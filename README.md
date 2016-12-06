# Setup Instructions

## Additional Data

You need some additional data for some modules:

    http://download.dimajix.net/training/amazon_baby.zip
    http://download.dimajix.net/training/bike-sharing.tgz
    http://download.dimajix.net/training/weather.tgz
    
## Zeppelin  
    
    http://download.dimajix.net/training/zeppelin-0.5.6-incubating.tgz


## Directory Structure

For easiest results, I recommend to have the following directory layout

/home/cloudera
         |
         +---- spark-training
         |        |
         |        +----- exameple-01
         |        |
         |        +----- exameple-02
         |        |
         |        ...
         |
         +---- data
                  |
                  +--- weather
                          |
                          +--- isd-history
                          +--- 2004
                          +--- 2005
                          .
                          .
                          +--- 2014
                          

## Building

mvn install

