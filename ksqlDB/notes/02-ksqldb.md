## ksqlDB Example

This is an example of using ksqlDB

- IVR
  - ()
- CIV
- Login

```bash
docker exec -it ksqldb ksql http://ksqldb:8088

# Setup streams and Postgres sink
run script scripts/01-create-input-streams.sql
run script scripts/02-create-sink.sql
run script scripts/03-produce-data.sql
```

## Query Kafka Stream

```bash
PRINT ivr from beginning;
```
