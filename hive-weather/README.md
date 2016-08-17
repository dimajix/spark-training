# Preparation

We need to upload the relevant data into HDFS.

```bash
hdfs dfs -mkdir -p data/weather
hdfs dfs -put data/weather/20* data/weather
hdfs dfs -mkdir -p data/weather/isd
hdfs dfs -put data/weather/isd-history data/weather/isd-history
```
