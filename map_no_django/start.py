import pika
import Cyph as cy

#from IA_predict import predict  # tu lógica IA aquí

# Conexión al servidor RabbitMQ
rabbit_host = '10.128.0.9'
rabbit_user = 'isis2503'
rabbit_password = '1234'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbit_host,
                              credentials=pika.PlainCredentials(rabbit_user, rabbit_password)))
channel = connection.channel()

# Asegurarse de que la cola exista
channel.queue_declare(queue='ia_requests', durable=True, exclusive=False, auto_delete=False)
channel.queue_declare(queue='map_requests', durable=True, exclusive=False, auto_delete=False)

def callback(ch, method, properties, body):
    from post import post
    entrada = cy.decrypt_json(body)
    post(entrada)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def publish(message):
    channel.basic_publish(
        exchange='',
        routing_key='ia_requests',
        body= cy.encrypt_json(message),
        properties=pika.BasicProperties(
            delivery_mode=2  # 1 = no persistente, 2 = persistente
        )
    )

# --> Escuchar los mensajes entrantes
channel.basic_consume(queue='map_requests', on_message_callback=callback)
print("Esperando mensajes...")
channel.start_consuming()