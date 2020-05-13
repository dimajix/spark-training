# Test Kafka

## Deploy
First we deploy a small Kafka cluster and Zookeeper cluster within Kubernetes.
```
kubectl apply -f namespace.yml 

kubectl apply -n dimajix -f zookeeper-svc.yml 
kubectl apply -n dimajix -f zookeeper-ss.yml 

kubectl apply -n dimajix -f kafka-svc.yml 
kubectl apply -n dimajix -f kafka-ss.yml 
```

## Create Producer
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

## Create Consumer
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
