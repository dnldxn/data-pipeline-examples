```bash
# Delete a topic
docker exec kafka kafka-topics --zookeeper zookeeper:32181 --delete --topic ivr

docker exec kafka kafka-topics --zookeeper zookeeper:32181 --list
```