# JDBC Example

This is the example uses the jdbc connectivity of SparkSQL to create and read from SQL tables.

## Preparation

You need to create an empty database in some MySQL server. This can be done via

    > mysql --user=root --password=cloudera --host=localhost

    CREATE TABLE training;
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY PASSWORD '*D997577481B722A2996B58BCE11EF3C312AC0B89' WITH GRANT OPTION;
    FLUSH PRIVILEGES;


## Running

First you need to export data from HDFS into MySQL.

    ./run_export.sh \
        --weather data/weather/20* \
        --stations data/weather/ish-history.csv \
        --dburi jdbc:mysql://localhost/training \
        --dbuser root \
        --dbpass cloudera

Then we can run the analytics part

    ./run_analyze.sh \
        --output data/weather_minmax \
        --dburi jdbc:mysql://localhost/training \
        --dbuser root \
        --dbpass cloudera
