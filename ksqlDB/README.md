# README

Weekend experiments with Kafka and ksqlDB.  We will build a stream application based on call center interactions, namely customer calls to an automated IVR (Interactive Voice Response) system and optional subsequent transfer to a human agent for servicing.

There will be 3 main data streams:
- IVR
- Agent
- Device

There is a simple Go program to produce random data to these topics.  Use `go run producer.go` to execute the program after initializing the Docker environment below.

Data is processed through several stream processors (created in scripts/03-stream-processors.sql) before being pushed to Postgres using Kafka connect.  From there it can be used for model training or scoring.

## Environment

The Docker compose file deploys the following components:
- Kafka Broker
- Zookeeper (currently required for Kafka)
- ksqlDB server (compute layer)
- Postgres Database (to consume results through Kafka Connect)

1. Start the above services by running the following command:

```bash
docker compose up -d
```

2. Make sure everything is running
```bash
docker compose ps
```

3. Once the services are running successfully, open another tab and pre-create the Kafka topic needed for this tutorial:

```bash
docker-compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --topic users \
    --replication-factor 1 \
    --partitions 4 \
    --create
```

4. Cleanup when done

```bash
# Stop the cluster
docker compose down

# Remove all Docker containers and volumes
docker compose rm -fv
```
