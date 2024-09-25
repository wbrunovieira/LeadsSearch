import os
import time
import json
import random
# pylint: disable=E0401
import pika
import requests
import http.client 

from dotenv import load_dotenv

from bs4 import BeautifulSoup

from urllib.parse import quote

load_dotenv()



def setup_rabbitmq():
    """Configura a conexão com o RabbitMQ."""
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    return connection

def setup_channel(connection):
    """Configura o canal RabbitMQ."""
    channel = connection.channel()
    exchange_name = 'leads_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    queue_name = 'scrapper_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name)
    return channel

def sanitize_input(input_str):
    """Remove caracteres especiais e espaços duplos, e prepara o input para ser usado na URL."""
    sanitized_str = input_str.strip()  # Remove espaços extras nas extremidades
    sanitized_str = ' '.join(sanitized_str.split())  # Remove espaços duplos ou mais
    return sanitized_str

def fetch_data_from_api(api_key, url):
    """Faz uma requisição à API de scraping com a URL especificada."""
    conn = http.client.HTTPSConnection("cheap-web-scarping-api.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "cheap-web-scarping-api.p.rapidapi.com"
    }
    
    encoded_url = quote(url)
    api_path = f"/scrape?url={encoded_url}"
    
    conn.request("GET", api_path, headers=headers)
    res = conn.getresponse()
    data = res.read()

    print(f"API Response: {data.decode('utf-8')}") 

    return data.decode("utf-8")


def parse_company_data(html_data):
    """Analisa os dados HTML retornados pela API e extrai as informações da empresa."""
    try:
        # Carregar o conteúdo HTML no BeautifulSoup
        soup = BeautifulSoup(html_data, 'html.parser')

        # Procurar pelo nome da empresa e CNPJ no HTML
        company_name_tag = soup.find('p', {'class': 'text-lg font-medium text-blue-600'})
        cnpj_tag = soup.find('p', {'class': 'text-sm text-gray-500'})
        
        if company_name_tag and cnpj_tag:
            company_name = company_name_tag.get_text(strip=True)
            company_cnpj = cnpj_tag.get_text(strip=True)
            return {'name': company_name, 'cnpj': company_cnpj}
        else:
            print("Detalhes da empresa não encontrados no HTML")
            return None
    except Exception as e:
        print(f"Erro ao processar HTML: {e}")
        return None


def callback(ch, method, properties, body):
    """Processa mensagens da fila RabbitMQ e realiza buscas."""
    try:
        lead_data = json.loads(body)
        print(f"Lead Data: {json.dumps(lead_data, indent=4, ensure_ascii=False)}")

        name = lead_data.get('Name')
        city = lead_data.get('City')

        sanitized_name = sanitize_input(name)
        sanitized_city = sanitize_input(city)
        if isinstance(name, str) and isinstance(city, str) and name.strip() and city.strip():
            print(f"Buscando informações para: {name}, {city}")
            
            # Monta a URL de busca para a API
            api_key = os.getenv('RAPIDAPI_KEY')
            search_url = f"https://cnpj.biz/procura/{quote(sanitized_name)}%20{quote(sanitized_city)}"
            
            print(f"Buscando na API a URL: {search_url}")
            try:
                response_data = fetch_data_from_api(api_key, search_url)
                company_info = parse_company_data(response_data)
            except Exception as e:
                print(f"Erro ao buscar dados da API: {str(e)}")
                company_info = None
            
            if company_info:
                print(f"Detalhes da Empresa: {company_info}")
            else:
                print("Nenhuma informação de empresa encontrada.")

        else:
            print("Nome ou Cidade não encontrados no lead.")
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")


def main():
    """Função principal para iniciar o serviço de scraping de leads."""
    connection = setup_rabbitmq()
    channel = setup_channel(connection)

    print(" [*] Esperando por mensagens. Para sair pressione CTRL+C")
    channel.basic_consume(queue='scrapper_queue', on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(" [x] Interrompido pelo usuário")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
