# Build Docker image
```
docker image build .
docker image tag <hash> dimajix-training/streaming-twitter
docker image save dimajix-training/streaming-twitter | pv | minikube ssh docker image load

docker image tag <hash> 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/streaming-twitter
docker image push 874361956431.dkr.ecr.eu-central-1.amazonaws.com/dimajix-training/streaming-twitter
```

# Test Kafka
```
kubectl -n dimajix \
    run \
    --generator=run-pod/v1 kafka-producer \
    -i \
    --tty \
    --rm \
    --image=wurstmeister/kafka:2.12-2.5.0 \
    --command \
    -- \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    --topic lala
```

```
kubectl -n dimajix \
    run \
    --generator=run-pod/v1 kafka-consumer \
    -i \
    --tty \
    --rm \
    --image=wurstmeister/kafka:2.12-2.5.0 \
    --command \
    -- \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    --topic lala \
    --from-beginning
```



# Fill in data
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

# Prepare Kubernetes

```
kubectl create serviceaccount spark -n dimajix
kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=dimajix:spark --namespace=dimajix
```
