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
kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=dimajix:spark -n dimajix
```

## Fill in data

Now we fill the Kafka topic `tweets` with some data, which is provided on S3.
```
scripts/kafka-twitter-producer.sh
```

## Consume tweets
Let us reconsume the topic, just to check that new data arrives.
```
scripts/kafka-console-consumer.sh --topic tweets  --from-beginning
```

## Consume results
Now let us also start consuming the (still empty) output topic.
```
scripts/kafka-console-consumer.sh --topic hashtags  --from-beginning
```

## Start Spark job
Finally we can simply start the Spark job via
```
scripts/run-streaming-twitter.sh --output hashtags
```
