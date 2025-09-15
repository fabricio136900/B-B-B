from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

consumer = KafkaConsumer(
    'btc-blocks',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

conn = psycopg2.connect(dbname="crypto_data", user="fabricio", password="fabricio.123")
cur = conn.cursor()

for msg in consumer:
    try:
        block = msg.value

        # Validaciones opcionales
        if 'id' not in block or 'height' not in block or 'timestamp' not in block:
            print(f"[Consumer Blocks] Bloque inválido: {block}")
            continue

        cur.execute("""
            INSERT INTO blocks (block_id, height, size, timestamp, tx_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (block_id) DO NOTHING;
        """, (
            block['id'],
            block['height'],
            block.get('size'),
            datetime.fromtimestamp(block['timestamp']),
            block.get('tx_count')  # Puede ser None, y eso está bien
        ))

        conn.commit()
        print(f"[Consumer Blocks] Insertado bloque {block['id']}")

    except Exception as e:
        conn.rollback()
        print(f"[Consumer Blocks] Error al insertar bloque: {e}")

