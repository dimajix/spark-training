hdfs dfs -mkdir alice
hdfs dfs -put data/alice-in-wonderland.txt alice

hadoop  jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper.py,reducer.py \
    -input alice \
    -output alice-wc \
    -mapper mapper.py \
    -reducer reducer.py

cat mapper.py | ./mapper.py | sort | ./reducer.py 

