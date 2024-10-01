import os
import json
import re

# pylint: disable=E0401
import pika

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from redis import Redis, exceptions as redis_exceptions
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

logging.info("Iniciando o serviço datalake")

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
    channel.queue_bind(exchange=exchange_name, queue='datalake_queue')

    channel.queue_declare(queue='linkfetcher_queue', durable=True)
    channel.queue_bind(exchange=exchange_name, queue='linkfetcher_queue')

    return channel

def setup_elasticsearch():
    """Configura a conexão com o Elasticsearch."""
    es_host = os.getenv('ELASTICSEARCH_HOST', 'elasticsearch')
    es_port = int(os.getenv('ELASTICSEARCH_PORT', '9200'))
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
   
    if not es.ping():
        print("Erro ao conectar ao Elasticsearch.")
        return None
    print("Conectado ao Elasticsearch com sucesso.")
    return es

def setup_redis():
    """Configura a conexão com o Redis."""
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', '6379'))
    
    redis_client = Redis(host=redis_host, port=redis_port, db=0)

    try:
        redis_client.ping()
        logging.info("Conexão com o Redis bem-sucedida!")
        return redis_client
    except redis_exceptions.ConnectionError as e:
        logging.error("Erro ao conectar ao Redis: %s", e)
        return None

def index_data_to_elasticsearch(es: Elasticsearch, index_name: str, lead_id: str, data: dict):
    """Indexa os dados recebidos no Elasticsearch."""
    try:
        data['lead_id'] = lead_id
        json_data = json.dumps(data)
        
        print(f"vamos salvar no elastic o lead_id {lead_id} com json_data {json_data}",lead_id)
        
        
        response = es.index(index=index_name, body=json_data)
        print(f"Dados indexados no Elasticsearch com sucesso: {response}")
    except json.JSONDecodeError as e:
        logging.error("Erro ao serializar os dados em JSON: %s", e)
    except TypeError as e:
        logging.error("Erro de tipo ao processar os dados: %s", e)
    except ConnectionError as e:
        logging.error("Erro de conexão ao tentar indexar os dados no Elasticsearch: %s", e)
    except es_exceptions.ElasticsearchException as e:
        logging.error("Erro do Elasticsearch ao indexar os dados: %s", e)
    except Exception as e:
        logging.error("Erro inesperado ao indexar no Elasticsearch: %s", e)


def get_lead_id_from_redis(redis_client, google_id):
    """Obtém o lead_id a partir do google_id no Redis."""
    try:
        redis_key = f"google_lead:{google_id}"

        lead_id = redis_client.get(redis_key)

        if lead_id:
            print(f"Lead ID {lead_id.decode('utf-8')} encontrado para o Google ID  no datalake{google_id}.")
            return lead_id.decode('utf-8')
        else:
            print(f"Lead ID não encontrado no Redis para o Google ID no datalake {google_id}.")
            return None
    except redis_exceptions.RedisError as e:
        logging.error("Erro ao buscar Lead ID no Redis: %s", e)
        return None

def save_to_elasticsearch(es, lead_id, content):
    """Salva o conteúdo extraído no Elasticsearch."""
    try:
        index_name = "leads_content"
        document = {
            "lead_id": lead_id,
            "content": content
        }
        logging.debug("Salvando no Elasticsearch: dados do visit link  %s", document)
        es.index(index=index_name, body=document)
        logging.info("Documento salvo no Elasticsearch dados do visit link: %s", document)
    except (ConnectionError, ValueError) as e:
        logging.error("Erro ao salvar no Elasticsearch: %s", e)
    except Exception as e:
        logging.error("Erro inesperado ao salvar no Elasticsearch: %s", e)

def clean_text(text):
    """Remove espaços vazios extras, múltiplos \n e \t e caracteres indesejados."""
  
    text = re.sub(r'[\n\t]+', ' ', text)
    

    text = re.sub(r'\s+', ' ', text)


    return text.strip()

def process_data_from_link_fetcher(body, es):
    """Processa os dados da fila 'fecher_link_queue' e salva no Elasticsearch."""
    try:
        logging.debug("Mensagem recebida da fila: (antes de decodificar): %s",body)

        if isinstance(body, bytes):
            body = body.decode('utf-8')

        logging.debug("Mensagem recebida da fila (depois de decodificar): %s", body)
        message = json.loads(body)
                     
                
        if 'lead_id' in message and 'content' in message:
            lead_id = message.get('lead_id')
            content = message.get('content')
            content = clean_text(content)
            logging.debug("Lead ID: %s, Content: %s", lead_id, content)

            if lead_id and content:
                logging.debug("Lead ID: %s, Content: %s", lead_id, content)
                save_to_elasticsearch(es, lead_id, content)
            else:
                logging.error("Mensagem inválida recebida: lead_id ou content ausente.")
        else:
            logging.error("Mensagem inválida recebida do link fetcher:%s", message)
    except json.JSONDecodeError:
        logging.error("Erro ao decodificar JSON na fila 'fecher_link_queue'. Mensagem: %s", body)
    except Exception as e:
        logging.error("Erro ao processar a mensagem da fila 'fecher_link_queue': %s", e)

def process_data_from_scrapper(body, es, redis_client):
    """Processa os dados recebidos do scrapper, busca o lead_id no Redis e salva no Elasticsearch."""
    try:            
            combined_data = json.loads(body)
            serper_data = combined_data.get('serper_data', {})
            companies_info = combined_data.get('companies_info', [])
            print(f"Dados recebidos no datalake: {json.dumps(combined_data, indent=4, ensure_ascii=False)}")
            
            
            for company in companies_info:
                google_id = company.get('google_id')
                print("vindo do google_id for company in companies_info",google_id)
                
               
                lead_id = get_lead_id_from_redis(redis_client, google_id)
                print("vindo do lead_id for company in companies_info",lead_id)
                
                if lead_id:
                    
                    index_name = 'leads_data'
                    print("vindo do lead_id for company in companies_info",lead_id)
                    
                    if 'rating' in serper_data and serper_data.get('rating') is not None:
                        try:
                            serper_data['rating'] = float(serper_data['rating'])  
                        except ValueError:
                            print(f"Erro ao converter rating para float: {serper_data['rating']}")
                
                    
                    data_to_index = {
                        'company_info': company,
                        'serper_data': serper_data
                    }
                    
                    index_data_to_elasticsearch(es, index_name, lead_id, data_to_index)
                                
                   
                else:
                    print(f"Lead ID não encontrado para o Google ID {google_id}. Dados não indexados.")
    except json.JSONDecodeError as e:
        logging.error("Erro ao decodificar JSON: %s", e)
    except KeyError as e:
        logging.error("Chave ausente no JSON: %s", e)
    except TypeError as e:
        logging.error("Erro de tipo no processamento de dados: %s", e)

def main():

    es = setup_elasticsearch()
    if not es:
        print("Encerrando o serviço devido à falha de conexão com o Elasticsearch.")
        return

    redis_client = setup_redis()
    if not redis_client:
        print("Encerrando o serviço devido à falha de conexão com o Redis.")
        return
    
    connection = setup_rabbitmq()
    channel = setup_channel(connection)


    def datalake_callback(ch, method, properties, body):
        """Callback para processar mensagens da fila."""
        process_data_from_scrapper(body, es, redis_client)

    def linkfetcher_callback(ch, method, properties, body):
        process_data_from_link_fetcher(body, es)

    channel.basic_consume(queue='datalake_queue', on_message_callback=datalake_callback, auto_ack=True)

    channel.basic_consume(queue='linkfetcher_queue', on_message_callback=linkfetcher_callback, auto_ack=True)

    channel.start_consuming()
    

    print(" [*] Esperando por mensagens no datalake. Para sair pressione CTRL+C")



if __name__ == "__main__":
    main()
