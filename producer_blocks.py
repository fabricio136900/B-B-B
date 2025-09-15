import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_recent_block(offset=2):
    """
    Devuelve un bloque con un pequeño desfase para garantizar sincronización con el consumer.
    """
    try:
        blocks = requests.get('https://blockstream.info/api/blocks').json()
        if len(blocks) >= offset:
            block_id = blocks[offset]['id']
        else:
            block_id = blocks[0]['id']
        block = requests.get(f'https://blockstream.info/api/block/{block_id}').json()
        return block
    except Exception as e:
        print(f"Error al obtener bloque: {e}")
        return None

while True:
    block = get_recent_block(offset=2)
    if block:
        block_data = {
            'id': block['id'],
            'height': block['height'],
            'size': block.get('size'),
            'timestamp': block['timestamp'],
            'tx_count': block.get('tx_count')
        }
        producer.send('btc-blocks', value=block_data)
        print(f"[Producer Blocks] Enviado bloque {block_data['id']}")
    time.sleep(30)

