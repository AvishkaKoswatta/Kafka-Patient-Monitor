KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo $KAFKA_CLUSTER_ID

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties --standalone

bin/kafka-server-start.sh config/server.properties



bin/connect-distributed.sh config/connect-distributed.properties


nano connector.json


curl -X POST -H "Content-Type: application/json" --data @connector.json http:/localhost:8083/connectors


pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.1


spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 spark_kafka_consumer.py