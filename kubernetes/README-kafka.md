# Test Kafka

## Deploy
First we deploy a small Kafka cluster and Zookeeper cluster within Kubernetes.
```
kubectl create namespace $(whoami) 

kubectl apply -f zookeeper-svc.yml 
kubectl apply -f zookeeper-ss.yml 

kubectl apply -f kafka-svc.yml 
kubectl apply -f kafka-ss.yml 
```

## Create Producer
```
scripts/kafka-console-producer --topic lala
```

## Create Consumer
```
scripts/kafka-console-consumer --topic lala --from-beginning
```
