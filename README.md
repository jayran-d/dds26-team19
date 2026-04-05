# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

## Project docs

- Codebase walkthrough: [docs/codebase_walkthrough.md](docs/codebase_walkthrough.md)
- Compose scaling guide: [docs/compose_scaling.md](docs/compose_scaling.md)

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 
 
K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.

## Container operations (docker compose)

Use these commands from the repo root.

### 1) Profiles and compose files

- small: project `dds-small`, file `docker/compose/docker-compose.small.yml`
- medium: project `dds-medium`, file `docker/compose/docker-compose.medium.yml`
- large: project `dds-large`, file `docker/compose/docker-compose.large.yml`

### 2) Quick status/log commands (all profiles)

```bash
make small-ps
make medium-ps
make large-ps

make small-logs
make medium-logs
make large-logs
```

### 3) Shell into containers (all profiles)

Small profile:

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-service sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec payment-service sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec stock-service sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-db sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec payment-db sh
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec stock-db sh
```

Medium profile:

```bash
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec order-service sh
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec payment-service sh
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec stock-service sh
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec order-db sh
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec payment-db sh
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec stock-db sh
```

Large profile:

```bash
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec order-service sh
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec payment-service sh
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec stock-service sh
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec order-db sh
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec payment-db sh
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec stock-db sh
```

### 4) Kill from inside the container (all profiles)

After shelling into any target container, run:

```bash
kill -TERM 1
```

This sends a graceful termination signal to the container's PID 1 process.

### 5) Non-interactive kill from host (all possible service/db targets)

Small profile:

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec payment-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec stock-service sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec order-db sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec payment-db sh -lc 'kill -TERM 1'
docker compose -p dds-small -f docker/compose/docker-compose.small.yml exec stock-db sh -lc 'kill -TERM 1'
```

Medium profile:

```bash
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec order-service sh -lc 'kill -TERM 1'
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec payment-service sh -lc 'kill -TERM 1'
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec stock-service sh -lc 'kill -TERM 1'
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec order-db sh -lc 'kill -TERM 1'
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec payment-db sh -lc 'kill -TERM 1'
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml exec stock-db sh -lc 'kill -TERM 1'
```

Large profile:

```bash
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec order-service sh -lc 'kill -TERM 1'
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec payment-service sh -lc 'kill -TERM 1'
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec stock-service sh -lc 'kill -TERM 1'
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec order-db sh -lc 'kill -TERM 1'
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec payment-db sh -lc 'kill -TERM 1'
docker compose -p dds-large -f docker/compose/docker-compose.large.yml exec stock-db sh -lc 'kill -TERM 1'
```

### 6) Host-side stop/kill/restart per container (all profiles)

Small profile:

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml stop order-service payment-service stock-service order-db payment-db stock-db
docker compose -p dds-small -f docker/compose/docker-compose.small.yml kill order-service payment-service stock-service order-db payment-db stock-db
docker compose -p dds-small -f docker/compose/docker-compose.small.yml restart order-service payment-service stock-service order-db payment-db stock-db
```

Medium profile:

```bash
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml stop order-service payment-service stock-service order-db payment-db stock-db
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml kill order-service payment-service stock-service order-db payment-db stock-db
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml restart order-service payment-service stock-service order-db payment-db stock-db
```

Large profile:

```bash
docker compose -p dds-large -f docker/compose/docker-compose.large.yml stop order-service payment-service stock-service order-db payment-db stock-db
docker compose -p dds-large -f docker/compose/docker-compose.large.yml kill order-service payment-service stock-service order-db payment-db stock-db
docker compose -p dds-large -f docker/compose/docker-compose.large.yml restart order-service payment-service stock-service order-db payment-db stock-db
```

### 7) Other general operations

Start or rebuild a profile:

```bash
make small-up-saga
make small-up-2pc
make medium-up-saga
make medium-up-2pc
make large-up-saga
make large-up-2pc
```

Tear down a profile completely (including volumes):

```bash
make small-down
make medium-down
make large-down
```

Tail logs for one specific container:

```bash
docker compose -p dds-small -f docker/compose/docker-compose.small.yml logs -f order-service
docker compose -p dds-medium -f docker/compose/docker-compose.medium.yml logs -f payment-db
docker compose -p dds-large -f docker/compose/docker-compose.large.yml logs -f stock-service
```
