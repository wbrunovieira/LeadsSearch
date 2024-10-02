import json
import os
import re

# pylint: disable=E0401
from langdetect import detect, LangDetectException
import aiohttp
import requests
import pika
from bs4 import BeautifulSoup
from redis import Redis
from urllib.parse import quote
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

load_dotenv()

logging.info("Iniciando o serviço linkfetcher")

allowed_languages = ['pt', 'en', 'es']


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
    logging.info("Conexão com RabbitMQ estabelecida com sucesso em %s:%s", rabbitmq_host, rabbitmq_port)
    return connection

def setup_channel(connection):
    """Configura o canal RabbitMQ para consumir dados do 'companies_exchange'."""
    channel = connection.channel()
    exchange_name = 'companies_exchange'
    
    
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    channel.queue_declare(queue='datalake_queue', durable=True)
    

    return channel

async def fetch_data_from_api(api_key, url):
    """Faz uma requisição assíncrona à API de scraping com a URL especificada."""
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "cheap-web-scarping-api.p.rapidapi.com"
    }
    encoded_url = quote(url)
    api_url = f"https://cheap-web-scarping-api.p.rapidapi.com/scrape?url={encoded_url}"

    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers) as response:
            print(f"[LOG] Status da resposta: {response.status}")
            if response.status == 200:
                    response_text = await response.text()

                    soup = BeautifulSoup(response_text, 'html.parser')
                    text_content = soup.get_text(separator=' ', strip=True)
                    text_content = clean_text(text_content)

                    try:
                    
                        language = detect(response_text)
                        logging.info("Idioma link fetche detectado: %s para o link %s", language, url)

                        
                        if language not in allowed_languages:
                            logging.warning("Ignorando o conteúdo do link fetche link %s devido ao idioma: %s", url, language)
                            return None

                        logging.info("[LOG] Dados recebidos com sucesso da API para link fetche o link %s.",url)
                        logging.info("[LOG] Dados recebidos com sucesso  no link fetcher da API para o response_text %response_text.",response_text)
                        
                        return response_text

                    except LangDetectException as e:
                        logging.error("Erro ao detectar o idioma para o link %s: %s", url, e)
                        return None


                    print(f"[LOG] Dados recebidos com sucesso da API: {response_text[:500]}...")  
                    return response_text
            else:
                    
                    print(f"[LOG] Erro na solicitação: Status {response.status}, motivo: {response.reason}")
                    return None
            return await response.text()

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
        logging.info("Dados enviados para o RabbitMQ do linkFetcher: %s", message)
    except pika.exceptions.UnroutableError as e:
        print(f"Erro ao publicar a mensagem: {e}")
    finally:
        # Fechar a conexão após o envio
        connection.close()
        print("Conexão com o RabbitMQ fechada.")


    
    logging.info(f"Enviando dados para o datalake: {data}")

def clean_text(text):
    """Remove espaços vazios extras, múltiplos \n e \t e caracteres indesejados."""
    
    text = re.sub(r'[\n\r\t]+', ' ', text)
    
    
    text = re.sub(r'\s+', ' ', text)

    
    return text.strip()


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

async def process_data_from_scrapper(body, redis_client):
    """Processa os dados recebidos do scrapper, busca o lead_id no Redis e consulta os links do serpro e envia para o data;ake salvar no elasticsearch"""
    try:            
            combined_data = json.loads(body)
            companies_info = combined_data.get('companies_info', [])
            serper_data = combined_data.get('serper_data', {})
            organic_results = serper_data.get('organic', [])

            api_key = os.getenv('RAPIDAPI_KEY')

            for company in companies_info:
                google_id = company.get('google_id')
                print("vindo do google_id for company in companies_info",google_id)
                
                
                lead_id = get_lead_id_from_redis(redis_client, google_id)
                print("vindo do lead_id for company in companies_info",lead_id)
            
            for i, result in enumerate(organic_results):
                if i >= 5:  
                    break
                link = result.get('link')
                if link:
                    print("link do extract",link)
                    content = await fetch_data_from_api(link,api_key)
                    print("content do extract",content)
                    if content:
                        content = clean_text(content)
                        send_to_datalake(lead_id, content)
                    else:
                        logging.error("Falha ao extrair conteúdo do link %s para o Google ID %s",link,google_id)

            companies_info = combined_data.get('companies_info', [])
            print(f"Dados recebidos no linkfeatcher do scrapper: {json.dumps(combined_data, indent=4, ensure_ascii=False)}")
            
            for company in companies_info:
                google_id = company.get('google_id')
                print("vindo do google_id for company in companies_info",google_id)
                
                
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

    async def callback(ch, method, properties, body):
        """Callback para processar mensagens da fila."""
     
        await process_data_from_scrapper(body, redis_client)
    
    channel.basic_consume(queue='datalake_queue', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    
    print(" [*] Esperando por mensagens no linkfetcher. Para sair pressione CTRL+C")

if __name__ == "__main__":
    main()