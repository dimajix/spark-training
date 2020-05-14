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
scripts/kafka-console-producer --topic lala
```

## Create Consumer
```
scripts/kafka-console-consumer --topic lala --from-beginning
```
