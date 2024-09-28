import json
import os

# pylint: disable=E0401
import requests
import pika
from bs4 import BeautifulSoup


# Configurações de RabbitMQ
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
LINK_QUEUE = 'links_to_process'

# Função para remover tags HTML e retornar o texto limpo
def fetch_and_clean_link(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Extrai o texto limpo (sem tags HTML)
            return soup.get_text(separator=' ', strip=True)
        else:
            print(f"Erro ao buscar a URL: {url}, Status: {response.status_code}")
            return None
    except Exception as e:
        print(f"Erro durante a requisição: {e}")
        return None

# Função para consumir links da fila do RabbitMQ
def consume_links():
    # Conectar ao RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
    channel = connection.channel()

    # Declarar a fila
    channel.queue_declare(queue=LINK_QUEUE, durable=True)

    def callback(ch, method, properties, body):
        link_data = json.loads(body)
        url = link_data.get('url')
        if url:
            print(f"Processando URL: {url}")
            # Buscar e limpar o conteúdo do link
            clean_text = fetch_and_clean_link(url)
            if clean_text:
                # Aqui você pode enviar o conteúdo limpo para outro serviço ou armazenar
                print(f"Texto Limpo:\n{clean_text[:200]}...")  # Exibe os primeiros 200 caracteres

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Consome a fila
    channel.basic_consume(queue=LINK_QUEUE, on_message_callback=callback, auto_ack=False)

    print(f"Esperando por mensagens na fila '{LINK_QUEUE}'...")
    channel.start_consuming()

if __name__ == "__main__":
    consume_links()
