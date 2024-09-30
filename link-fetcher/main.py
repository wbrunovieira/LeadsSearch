import json
import os

# pylint: disable=E0401
import requests
import pika
from bs4 import BeautifulSoup
from redis import Redis
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

logging.info("Iniciando o serviço linkfetcher")


def setup_redis():
    """Configura a conexão com o Redis."""
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    
    # Cria o cliente Redis
    redis_client = Redis(host=redis_host, port=redis_port, db=0)

    try:
        # Testa a conexão com o Redis
        redis_client.ping()
        response = redis_client.ping()
        print(response)
        if redis_client.ping():
            print("Conexão com o Redis bem-sucedida!")
        else:
            print("Falha na conexão com o Redis.")
        logging.info("Conectado ao Redis em %s:%s", redis_host, redis_port)

        return redis_client
        
    except Exception as e:
        logging.error("Erro ao conectar ao Redis em%s:%s:%s", redis_host,redis_port, e)
        return None
    return redis_client


def setup_rabbitmq():
    """Configura a conexão com o RabbitMQ para o datalake."""
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    return connection

def setup_channel(connection):
    """Configura o canal RabbitMQ para consumir dados do 'companies_exchange'."""
    channel = connection.channel()
    exchange_name = 'companies_exchange'
    
    
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    channel.queue_declare(queue='datalake_queue', durable=True)
    

    return channel

def extract_content_from_link(link):
    """Extrai o conteúdo do link removendo as tags HTML."""
    try:
        response = requests.get(link)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            print("soup.get_text()",soup.get_text())
            return soup.get_text()  # Extrai apenas o texto, sem as tags HTML
        else:
            logging.error(f"Erro ao acessar o link {link}: Status Code {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Erro ao extrair conteúdo do link {link}: {e}")
        return None

def send_to_datalake(lead_id, content):
    """Envia o conteúdo extraído para o datalake."""
    data = {
        'lead_id': lead_id,
        'content': content
    }

    connection = setup_rabbitmq()
    channel = setup_channel(connection)
    exchange_name = 'companies_exchange'

    try:
       
        message = json.dumps(content)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key='',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensagem persistente
            )
        )
        print(f"Dados enviados para o RabbitMQ: {content}")
    except pika.exceptions.UnroutableError as e:
        print(f"Erro ao publicar a mensagem: {e}")
    finally:
        # Fechar a conexão após o envio
        connection.close()
        print("Conexão com o RabbitMQ fechada.")


    
    logging.info(f"Enviando dados para o datalake: {data}")


def get_lead_id_from_redis(redis_client, google_id):
    """Obtém o lead_id a partir do google_id no Redis."""
    try:
        redis_key = f"google_lead:{google_id}"

        lead_id = redis_client.get(redis_key)

        if lead_id:
            print(f"Lead ID {lead_id.decode('utf-8')} encontrado para o Google ID {google_id}.")
            return lead_id.decode('utf-8')
        else:
            print(f"Lead ID não encontrado no Redis para o Google ID {google_id}.")
            return None
    except Exception as e:
        print(f"Erro ao buscar Lead ID no Redis: {e}")
        return None

def process_data_from_scrapper(body, redis_client):
    """Processa os dados recebidos do scrapper, busca o lead_id no Redis e consulta os links do serpro e envia para o data;ake salvar no elasticsearch"""
    try:            
            combined_data = json.loads(body)
            companies_info = combined_data.get('companies_info', [])
            serper_data = combined_data.get('serper_data', {})
            organic_results = serper_data.get('organic', [])

            for company in companies_info:
                google_id = company.get('google_id')
                print("vindo do google_id for company in companies_info",google_id)
                
                # Busca o lead_id no Redis
                lead_id = get_lead_id_from_redis(redis_client, google_id)
                print("vindo do lead_id for company in companies_info",lead_id)

            for result in organic_results:
                link = result.get('link')
                if link:
                    print("link",link)
                    content = extract_content_from_link(link)
                    print("content",content)
                    if content:
                        send_to_datalake(lead_id, content)
                    else:
                        logging.error(f"Falha ao extrair conteúdo do link {link} para o Google ID {google_id}")

            companies_info = combined_data.get('companies_info', [])
            print(f"Dados recebidos no datalake: {json.dumps(combined_data, indent=4, ensure_ascii=False)}")
            
            for company in companies_info:
                google_id = company.get('google_id')
                print("vindo do google_id for company in companies_info",google_id)
                
                # Busca o lead_id no Redis
                lead_id = get_lead_id_from_redis(redis_client, google_id)
                print("vindo do lead_id for company in companies_info",lead_id)
            
                                                   
                    
       
    except json.JSONDecodeError:
        print("Erro ao decodificar JSON.")
    except Exception as e:
        print(f"Erro no processamento de dados: {e}")

def main():
    
    redis_client = setup_redis()
    if not redis_client:
        print("Encerrando o serviço devido à falha de conexão com o Redis.")
        return

    connection = setup_rabbitmq()
    channel = setup_channel(connection)

    def callback(ch, method, properties, body):
        """Callback para processar mensagens da fila."""
        process_data_from_scrapper(body, redis_client)
    
    channel.basic_consume(queue='datalake_queue', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    
    print(" [*] Esperando por mensagens no linkfetcher. Para sair pressione CTRL+C")

if __name__ == "__main__":
    main()