import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_recent_block_hash(offset=2):
    """
    Obtiene el hash del bloque con un desfase (por defecto el 2º más reciente)
    para evitar errores de sincronización con el consumer de bloques.
    """
    blocks = requests.get('https://blockstream.info/api/blocks').json()
    if len(blocks) >= offset:
        return blocks[offset]['id']
    return blocks[0]['id']  # fallback al más reciente si no hay suficientes

def get_txids(block_hash):
    return requests.get(f'https://blockstream.info/api/block/{block_hash}/txids').json()

def get_transaction(txid):
    return requests.get(f'https://blockstream.info/api/tx/{txid}').json()

while True:
    try:
        block_hash = get_recent_block_hash(offset=2)
        txids = get_txids(block_hash)
        for txid in txids[:3]:  # Limitar para pruebas
            tx = get_transaction(txid)
            tx_data = {
                'transaction': {
                    'txid': tx['txid'],
                    'size': tx.get('size'),
                    'weight': tx.get('weight'),
                    'version': tx.get('version'),
                    'fee': tx.get('fee'),
                    'block_id': block_hash,
                    'block_number': tx.get('status', {}).get('block_height'),
                    'timestamp': tx.get('status', {}).get('block_time')
                },
                'inputs': [{
                    'prev_txid': vin.get('txid'),
                    'prev_index': vin.get('vout'),
                    'value': vin.get('prevout', {}).get('value'),
                    'address': vin.get('prevout', {}).get('scriptpubkey_address'),
                    'sequence': vin.get('sequence')
                } for vin in tx.get('vin', []) if vin.get('prevout')],
                'outputs': [{
                    'index': vout.get('n'),
                    'value': vout.get('value'),
                    'address': vout.get('scriptpubkey_address')
                } for vout in tx.get('vout', []) if vout.get('scriptpubkey_address')]
            }
            producer.send('btc-transactions', value=tx_data)
            print(f"[Producer TX] Enviada tx {txid}")
        time.sleep(30)
    except Exception as e:
        print(f"Error en producer_transactions: {e}")
        time.sleep(10)

