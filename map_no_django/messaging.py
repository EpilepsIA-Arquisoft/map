import pika
import Cyph as cy

# RabbitMQ connection settings
rabbit_host = '10.128.0.20'
rabbit_user = 'isis2503'
rabbit_password = '1234'

# Establish a dedicated connection for publishing
def _create_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbit_host,
            credentials=pika.PlainCredentials(rabbit_user, rabbit_password)
        )
    )

_publish_conn = _create_connection()
_publish_channel = _publish_conn.channel()
_publish_channel.queue_declare(queue='ia_requests', durable=True)


def publish(message: dict, queue: str = 'ia_requests') -> None:
    """
    Publishes a JSON-serializable dict to the specified RabbitMQ queue.
    """
    _publish_channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=cy.encrypt_json(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )