from kafka import KafkaConsumer
import psycopg2
import json
from datetime import datetime

consumer = KafkaConsumer(
    'btc-transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

conn = psycopg2.connect(dbname="crypto_data", user="fabricio", password="fabricio.123")
cur = conn.cursor()

for msg in consumer:
    data = msg.value
    tx = data['transaction']
    inputs = data['inputs']
    outputs = data['outputs']

    # Validar que el bloque existe antes de insertar la transacción
    cur.execute("SELECT 1 FROM blocks WHERE block_id = %s", (tx['block_id'],))
    if cur.fetchone() is None:
        print(f"[Consumer TX] Bloque {tx['block_id']} no encontrado. Saltando tx {tx['txid']}.")
        continue

    # Insertar transacción
    cur.execute("""
        INSERT INTO transactions (txid, size, weight, version, fee, block_id, block_number, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (txid) DO NOTHING;
    """, (
        tx['txid'],
        tx['size'],
        tx['weight'],
        tx['version'],
        tx['fee'],
        tx['block_id'],
        tx['block_number'],
        datetime.fromtimestamp(tx['timestamp']) if tx['timestamp'] else None
    ))

    # Insertar entradas (inputs)
    for i in inputs:
        cur.execute("""
            INSERT INTO inputs (txid, prev_txid, prev_index, value, address, sequence)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            tx['txid'],
            i['prev_txid'],
            i['prev_index'],
            i['value'],
            i['address'],
            i['sequence']
        ))

    # Insertar salidas (outputs)
    for o in outputs:
        cur.execute("""
            INSERT INTO outputs (txid, index, value, address)
            VALUES (%s, %s, %s, %s);
        """, (
            tx['txid'],
            o['index'],
            o['value'],
            o['address']
        ))

    conn.commit()
    print(f"[Consumer TX] Insertada tx {tx['txid']}")

