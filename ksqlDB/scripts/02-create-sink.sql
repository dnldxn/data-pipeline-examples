CREATE SINK CONNECTOR sink_postgres 
WITH(
    "connector.class" = 'io.confluent.connect.jdbc.JdbcSinkConnector',
    "connection.url" = 'jdbc:postgresql://db:5432/calls',
    "connection.user" = 'postgres',
    "connection.password" = 'postgres',
    "topics" = 'ivr',
    "auto.create" = 'true',
    "batch.size" = '1'
)
;

CREATE SINK CONNECTOR sink_postgres_civ
WITH(
    "connector.class" = 'io.confluent.connect.jdbc.JdbcSinkConnector',
    "connection.url" = 'jdbc:postgresql://db:5432/calls',
    "connection.user" = 'postgres',
    "connection.password" = 'postgres',
    "topics" = 'agent',
    "auto.create" = 'true',
    "batch.size" = '1'
)
;
