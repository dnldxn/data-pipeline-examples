## Console Producer - Consumer Example

Simple example showing how to use the built-in Kakfa CLI commands.  These can be run from the broker container.

```bash
docker exec -it kafka /bin/bash

# Produce data to topic
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --property key.separator=, \
  --property parse.key=true \
  --topic test

# Describe topic (in a second terminal window)
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test
  
# Consume from topic (from terminal window #2)
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test \
  --property print.timestamp=true \
  --property print.key=true \
  --property print.value=true \
  --from-beginning
```
