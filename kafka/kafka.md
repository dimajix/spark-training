# Create Messages
/opt/kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 \
    --topic lala

# Consume Messages
/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic lala \
    --from-beginning

# List Topics
/opt/kafka/bin/kafka-topics.sh \
    --zookeeper localhost:2181 \
    --list

# Weather Data
./s3cat.py s3://dimajix-training/data/weather-sample/ \
   | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic weather

/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic weather \
    --from-beginning

/opt/kafka/bin/kafka-topics.sh \
    --zookeeper localhost:2181 \
    --topic weather \
    --delete


# Twitter Data
./s3cat.py s3://dimajix-training/data/twitter-sample/ \
   | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic twitter

/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic twitter \
    --from-beginning

/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --topic twitter --delete


# Alice in Wonderland

./s3cat.py -T -I1 -B10 s3://dimajix-training/data/alice/alice-in-wonderland.txt \
   | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic alice

/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic alice \
    --from-beginning

/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --topic alice --delete

