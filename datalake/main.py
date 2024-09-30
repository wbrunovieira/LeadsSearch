import os
import json

# pylint: disable=E0401
import pika
from elasticsearch import Elasticsearch
from redis import Redis
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
    
    # Testa a conexão
    if not es.ping():
        print("Erro ao conectar ao Elasticsearch.")
        return None
    print("Conectado ao Elasticsearch com sucesso.")
    return es

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

def index_data_to_elasticsearch(es: Elasticsearch, index_name: str, lead_id: str, data: dict):
    """Indexa os dados recebidos no Elasticsearch."""
    try:
        data['lead_id'] = lead_id
        json_data = json.dumps(data)
        
        print(f"vamos salvar no elastic o lead_id {lead_id} com json_data {json_data}",lead_id)
        
        
        response = es.index(index=index_name, body=json_data)
        print(f"Dados indexados no Elasticsearch com sucesso: {response}")
    except Exception as e:
        print(f"Erro ao indexar os dados no Elasticsearch: {e}")

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

def save_to_elasticsearch(es, lead_id, content):
    """Salva o conteúdo extraído no Elasticsearch."""
    try:
        index_name = "leads_data"
        document = {
            "lead_id": lead_id,
            "content": content
        }
        es.index(index=index_name, body=document)
        logging.info(f"Documento salvo no Elasticsearch: {document}")
    except Exception as e:
        logging.error(f"Erro ao salvar no Elasticsearch: {e}")

def process_data_from_link_fetcher(body, es):
    """Processa os dados da fila 'fecher_link_queue' e salva no Elasticsearch."""
    try:
        message = json.loads(body)
        lead_id = message.get('lead_id')
        content = message.get('content')
        print("message lead_id ,content",message,lead_id,content)
        
        if lead_id and content:
            save_to_elasticsearch(es, lead_id, content)
        else:
            logging.error(f"Mensagem inválida recebida do link fetcher: {message}")
    except json.JSONDecodeError:
        logging.error("Erro ao decodificar JSON na fila 'fecher_link_queue'.")  
    except Exception as e:
        logging.error(f"Erro ao processar a mensagem da fila 'fecher_link_queue': {e}")

def process_data_from_scrapper(body, es, redis_client):
    """Processa os dados recebidos do scrapper, busca o lead_id no Redis e salva no Elasticsearch."""
    try:            
            combined_data = json.loads(body)
            serper_data = combined_data.get('serper_data', {})
            companies_info = combined_data.get('companies_info', [])
            print(f"Dados recebidos no datalake: {json.dumps(combined_data, indent=4, ensure_ascii=False)}")
            
            # Itera sobre companies_info para pegar o google_id e buscar o lead_id no Redis
            for company in companies_info:
                google_id = company.get('google_id')
                print("vindo do google_id for company in companies_info",google_id)
                
                # Busca o lead_id no Redis
                lead_id = get_lead_id_from_redis(redis_client, google_id)
                print("vindo do lead_id for company in companies_info",lead_id)
                
                if lead_id:
                    # Se o lead_id for encontrado, salva os dados no Elasticsearch
                    index_name = 'leads_data'
                    print("vindo do lead_id for company in companies_info",lead_id)
                    
                    if 'rating' in serper_data and serper_data.get('rating') is not None:
                        try:
                            serper_data['rating'] = float(serper_data['rating'])  # Garantir que 'rating' seja convertido para float
                        except ValueError:
                            print(f"Erro ao converter rating para float: {serper_data['rating']}")
                
                    
                    data_to_index = {
                        'company_info': company,
                        'serper_data': serper_data
                    }
                    
                    index_data_to_elasticsearch(es, index_name, lead_id, data_to_index)
                                
                    # Inclui o serper_data nos dados que serão indexados
                else:
                    print(f"Lead ID não encontrado para o Google ID {google_id}. Dados não indexados.")
    except json.JSONDecodeError:
        print("Erro ao decodificar JSON.")
    except Exception as e:
        print(f"Erro no processamento de dados: {e}")

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
