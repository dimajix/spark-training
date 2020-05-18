# Build Docker image

First we build a Docker image from `spark-scala/streaming-twitter`. After having built the whole repository, we can
create and push a Docker image as follows. This will then be used for running the application in Kubernetes

```
docker image tag dimajix-training/streaming-twitter:1.0.0 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/twitter-streaming
docker image push 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/twitter-streaming

docker image tag dimajix-training/hashtag2mysql:1.0.0 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/hashtag2mysql
docker image push 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/hashtag2mysql
```


# Preparation

## Prepare Kubernetes
First we need to create a so called *service account* and grant permissions to create new pods in our namespace.

```
kubectl create serviceaccount spark
kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=$(whoami):spark
```

## Deploy MariaDB
```
kubectl apply -f manifests/maria-svc.yml
kubectl apply -f manifests/maria-pv.yml
kubectl apply -f manifests/maria-deploy.yml
```

## Deploy Hashtag2Mysql
```
kubectl apply -f manifests/hashtag2mysql-deploy.yml
```


# Spark Demo

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
export PATH=$PATH:/opt/spark/bin
scripts/run-streaming-twitter.sh --output hashtags
```

## Peek into Database
First create a port forwarding to localhost
```
kubectl port-forward svc/mysql 3306
```
Now connect via MySQL client
```
mariadb -h 127.0.0.1 -P 3306 -u root --password=root_pwd
```
