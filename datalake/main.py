import os
import json

# pylint: disable=E0401
import pika
from elasticsearch import Elasticsearch

# Conectar ao Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])

# Conectar ao RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Consome mensagens da fila
def callback(ch, method, properties, body):
    data = json.loads(body)
    index_data_to_elasticsearch(data)

def index_data_to_elasticsearch(data):
    # Indexa o documento no Elasticsearch
    es.index(index='leads', body=data)

# Consome a fila 'data_queue'
channel.basic_consume(queue='data_queue', on_message_callback=callback, auto_ack=True)

print('Esperando por mensagens...')
channel.start_consuming()
