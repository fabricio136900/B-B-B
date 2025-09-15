#!/bin/bash

echo "Buscando procesos de Kafka y Zookeeper..."

# Buscar proceso de Kafka
KAFKA_PID=$(ps aux | grep 'kafka.Kafka' | grep -v grep | awk '{print $2}')

# Buscar proceso de Zookeeper
ZOOKEEPER_PID=$(ps aux | grep 'zookeeper' | grep -v grep | awk '{print $2}')

# Detener Kafka si está corriendo
if [ -n "$KAFKA_PID" ]; then
    echo "Deteniendo Kafka (PID $KAFKA_PID)..."
    kill -9 "$KAFKA_PID"
else
    echo "✔ Kafka no se está ejecutando."
fi

# Detener Zookeeper si está corriendo
if [ -n "$ZOOKEEPER_PID" ]; then
    echo "Deteniendo Zookeeper (PID $ZOOKEEPER_PID)..."
    kill -9 "$ZOOKEEPER_PID"
else
    echo "✔ Zookeeper no se está ejecutando."
fi

echo "Servicios detenidos correctamente."
