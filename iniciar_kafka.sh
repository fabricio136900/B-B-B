#!/bin/bash

KAFKA_DIR=~/kafka
ZOOKEEPER_PORT=2181
KAFKA_PORT=9092

echo "Verificando si Zookeeper está corriendo..."
if ! lsof -i :$ZOOKEEPER_PORT >/dev/null; then
    echo "Iniciando Zookeeper..."
    nohup $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties > ~/zookeeper.log 2>&1 &
    sleep 5
else
    echo "✔ Zookeeper ya está corriendo."
fi

echo "Verificando si Kafka está corriendo..."
if ! lsof -i :$KAFKA_PORT >/dev/null; then
    echo "Iniciando Kafka..."
    nohup $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties > ~/kafka.log 2>&1 &
    sleep 10
else
    echo "✔ Kafka ya está corriendo."
fi

echo "Verificando topics existentes..."
EXISTING_TOPICS=$($KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)

create_topic_if_not_exists() {
    TOPIC_NAME=$1
    if echo "$EXISTING_TOPICS" | grep -q "^$TOPIC_NAME$"; then
        echo "✔ Topic '$TOPIC_NAME' ya existe."
    else
        echo "Creando topic '$TOPIC_NAME'..."
        $KAFKA_DIR/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
            --topic "$TOPIC_NAME" --partitions 1 --replication-factor 1
    fi
}

create_topic_if_not_exists "btc-blocks"
create_topic_if_not_exists "btc-transactions"

echo "Todo listo. Zookeeper, Kafka y los topics están funcionando correctamente."
