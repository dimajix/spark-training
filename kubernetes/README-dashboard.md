# Installation
Install Dashboard via
```
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.3.6/components.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.0/aio/deploy/recommended.yaml
kubectl create serviceaccount eks-admin -n kube-system
kubectl create clusterrolebinding eks-admin --clusterrole cluster-admin --serviceaccount kube-system:eks-admin
```

# Usage
Start Kubernetes proxy via
```
kubectl proxy
```

Retrieve secret via
```
kubectl -n kube-system describe secret $(kubectl -n kube-system get secret | grep eks-admin | awk '{print $1}')
```

Login to dashboard at
```
http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#!/login
```
