# Build Docker image

First we build a Docker image from `spark-scala/streaming-twitter`. After having built the whole repository, we can
create and push a Docker image as follows. This will then be used for running the application in Kubernetes

```
docker image build .
docker image tag <hash> 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/twitter-streaming
docker image push 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/twitter-streaming
```

# Demo

## Prepare Kubernetes
First we need to create a so called *service account* and grant permissions to create new pods in our namespace.

```
kubectl create serviceaccount spark -n dimajix
kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=dimajix:spark --namespace=dimajix
```

## Fill in data

Now we fill the Kafka topic `tweets` with some data, which is provided on S3.
```
 ./s3cat.py s3://dimajix-training/data/twitter-sample/ \
    | kubectl -n dimajix \
          run \
          --generator=run-pod/v1 kafka-producer-tweets \
          -i \
          --rm \
          --image=wurstmeister/kafka:2.12-2.5.0 \
          --command \
          -- \
          /opt/kafka/bin/kafka-console-producer.sh \
          --bootstrap-server kafka-bootstrap:9092 \
          --topic tweets
```

## Consume tweets
Let us reconsume the topic, just to check that new data arrives.
```
kubectl -n dimajix \
    run \
    --generator=run-pod/v1 kafka-consumer-tweets \
    -i \
    --tty \
    --rm \
    --image=wurstmeister/kafka:2.12-2.5.0 \
    --command \
    -- \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    --topic tweets \
    --from-beginning
```

## Consume results
Now let us also start consuming the (still empty) output topic.
```
kubectl -n dimajix \
    run \
    --generator=run-pod/v1 kafka-hashtag-consumer-hashtags \
    -i \
    --tty \
    --rm \
    --image=wurstmeister/kafka:2.12-2.5.0 \
    --command \
    -- \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    --topic hashtags \
    --from-beginning
```

## Start Spark job
Finally we can simply start the Spark job via
```
./run-streaming-twitter.sh --output <topic>
```
