-- IVR Streams

CREATE STREAM ivr_csv (
    call_id VARCHAR KEY,
    call_dttm VARCHAR,
    cust_id INT,
    event_progress VARCHAR
) WITH (
    KAFKA_TOPIC = 'ivr-csv',
    VALUE_FORMAT = 'DELIMITED',
    PARTITIONS = 1,
    timestamp = 'call_dttm',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
)
;

CREATE STREAM ivr 
WITH (
    VALUE_FORMAT = 'AVRO',
    KAFKA_TOPIC = 'ivr'
) AS SELECT * FROM ivr_csv
;


-- Agent Streams

CREATE STREAM agent_csv (
    call_id VARCHAR KEY,
    event_progress VARCHAR
) WITH (
    KAFKA_TOPIC='agent-csv',
    VALUE_FORMAT='DELIMITED',
    PARTITIONS = 1,
    timestamp = 'call_dttm',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
);

CREATE STREAM agent
WITH (
    VALUE_FORMAT = 'AVRO',
    KAFKA_TOPIC = 'agent'
) AS SELECT * FROM agent_csv
;


-- Device Streams

CREATE STREAM device_csvcsv (
    device_id INT KEY,
    cust_id INT
) WITH (
    KAFKA_TOPIC='device-csv',
    VALUE_FORMAT='DELIMITED',
    PARTITIONS = 1
);

CREATE STREAM device (
    device_id INT KEY,
    cust_id INT
) WITH (
    KAFKA_TOPIC='device',
    VALUE_FORMAT='AVRO',
    PARTITIONS = 1
);
