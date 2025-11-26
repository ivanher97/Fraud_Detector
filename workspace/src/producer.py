import json
import datetime
import time
import random
from confluent_kafka import Producer
from faker import Faker

# Configuración: En un proyecto real, esto va en un .env
CONF = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'python-producer-1'
}

TOPIC = 'transactions'

CATEGORIES = ['Electronics', 'Jewelry', 'Travel', 'Food', 'Retail']

fake = Faker()

def delivery_report(err, msg):
    """
    Callback que se ejecuta cuando Kafka confirma la recepción (o fallo).
    Este es tu mecanismo de control de calidad.
    """
    if err is not None:
        print(f'❌ Error enviando mensaje: {err}')
    else:
        # Nota el uso de msg.topic() y msg.partition()
        print(f'✅ Mensaje entregado a {msg.topic()} [{msg.partition()}]')

def generate_transaction():
    """
    Genera un diccionario con datos simulados.
    Campos obligatorios: id_transaccion, tarjeta, cantidad, timestamp, comercio.
    """
    return {
        "id": fake.uuid4(),
        "card_id": fake.credit_card_number(card_type=None),
        "amount": round(random.uniform(10, 5000), 2), # Algunos altos para simular fraude
        "timestamp": datetime.datetime.now().isoformat(),
        "merchant_id": fake.company(),
        "category": random.choice(CATEGORIES),
        "location": {
            "lat": float(fake.latitude()),
            "lon": float(fake.longitude())
        }
    }

def main():
    # 1. Crear Instancia del Producer
    producer = Producer(CONF)

    print("🚀 Iniciando simulador de transacciones...")

    try:
        while True:
            # 2. Generar dato
            transaction = generate_transaction()

            # 3. Serializar a JSON
            transaction_bytes = json.dumps(transaction).encode('utf-8')

            # 4. Enviar
            producer.produce(TOPIC, transaction_bytes, callback = delivery_report)

            # 5. Forzar al producer a procesar los eventos de callback pendientes
            producer.poll(0) 

            time.sleep(0.5) # 2Hz aprox

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo simulador...")
    finally:
        # 6. Asegurar que todo lo que está en memoria se envíe antes de cerrar
        producer.flush()

if __name__ == '__main__':
    main()