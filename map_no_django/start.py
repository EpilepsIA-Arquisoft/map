import pika
import time
import Cyph as cy
from post import post

# Configuración de RabbitMQ
rabbit_host = '10.128.0.9'
rabbit_user = 'isis2503'
rabbit_password = '1234'

def create_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbit_host,
            credentials=pika.PlainCredentials(rabbit_user, rabbit_password),
            heartbeat=60,  # mantener la conexión viva
            blocked_connection_timeout=300  # tiempo máximo bloqueado
        )
    )

def callback(ch, method, properties, body):
    try:
        entrada = cy.decrypt_json(body)
        post(entrada)
    except Exception as e:
        print(f"[ERROR] Falló el procesamiento del mensaje: {e}")
    finally:
        # Reconoce el mensaje para que no se reenvíe
        ch.basic_ack(delivery_tag=method.delivery_tag)

def publish(channel, message):
    channel.basic_publish(
        exchange='',
        routing_key='ia_requests',
        body=cy.encrypt_json(message),
        properties=pika.BasicProperties(delivery_mode=2)  # persistente
    )

def start_consuming():
    while True:
        try:
            connection = create_connection()
            channel = connection.channel()

            # Declarar colas
            channel.queue_declare(queue='ia_requests', durable=True, exclusive=False, auto_delete=False)
            channel.queue_declare(queue='map_requests', durable=True, exclusive=False, auto_delete=False)

            channel.basic_consume(queue='map_requests', on_message_callback=callback)
            print("[INFO] Esperando mensajes...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[REINTENTANDO] Conexión perdida: {e}. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[ERROR FATAL] {e}. Reintentando en 5 segundos...")
            time.sleep(5)

if __name__ == "__main__":
    start_consuming()
