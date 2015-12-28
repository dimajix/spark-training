# SparkOnHbase Example

This is the example uses the SparkOnHBase package.

## Preparation

You need to create an appropriate HBase table before running the example. This can be done within the HBase shell.

create_namespace 'training'
create 'training:weather', 'cf'


## Running

    ./run-export.sh --weather data/weather/2011
