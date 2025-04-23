import pika
import json

# Conexión al servidor RabbitMQ
rabbit_host = '10.128.0.16'
rabbit_user = 'isis2503'
rabbit_password = '1234'
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbit_host,
                              credentials=pika.PlainCredentials(rabbit_user, rabbit_password)))
channel = connection.channel()

# Asegúrate de que la cola exista
channel.queue_declare(queue='map_requests', durable=True, exclusive=False, auto_delete=False)

# Mensaje de prueba (ajusta la ruta del archivo GCS a uno real)
mensaje = {
    "id_paciente": "12345",
    "id_examen": "test4",
    "ubicacion_examen": "https://storage.googleapis.com/examenes-eeg/examenes/p15_Record4.edf"
}

# Publica el mensaje
channel.basic_publish(
    exchange='',
    routing_key='map_requests',
    body=json.dumps(mensaje),
    properties=pika.BasicProperties(
        delivery_mode=2  # 1 = no persistente, 2 = persistente
    )
)

print("Mensaje de prueba enviado.")

connection.close()
