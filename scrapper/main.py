
# pylint: disable=E0401
import os
import json
import re
import asyncio
import aiohttp
import pika
import http.client
from urllib.parse import quote
from bs4 import BeautifulSoup
from dotenv import load_dotenv

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
    companies_exchange_name = 'companies_exchange'
    scrapper_exchange_name = 'scrapper_exchange'

    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    channel.exchange_declare(exchange=companies_exchange_name, exchange_type='fanout', durable=True)
    channel.exchange_declare(exchange=scrapper_exchange_name, exchange_type='fanout', durable=True)

    queue_name = 'scrapper_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    confirmation_queue_name = 'scrapper_confirmation_queue'
    channel.queue_declare(queue=confirmation_queue_name, durable=True)
    channel.queue_bind(exchange=scrapper_exchange_name, queue=confirmation_queue_name)

    return channel

async def fetch_serper_data(name, city):
    """Faz uma requisição à API Google Serper para buscar dados com base no nome e cidade."""
    conn = http.client.HTTPSConnection("google.serper.dev")
    
    payload = json.dumps({
      "q": f"{name}, {city}",  # Substitui o nome e cidade dinamicamente
      "gl": "br",  # Localização geográfica (Brasil)
      "hl": "pt-br",  # Idioma (Português)
      "num": 30  # Número de resultados desejados
    })
    
    headers = {
        'X-API-KEY': os.getenv('SERPER_API_KEY'), 
        'Content-Type': 'application/json'
    }

    conn.request("POST", "/search", payload, headers)
    res = conn.getresponse()
    data = res.read()
    
    # Retorna o conteúdo decodificado
    return data.decode("utf-8")

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
            return await response.text()

async def fetch_cnpj_data(cnpj):
    """Consulta dados do CNPJ usando a API da Invertexto de forma assíncrona."""
    try:
        api_token = os.getenv('INVERTEXTO_API_TOKEN')
        api_url = f"https://api.invertexto.com/v1/cnpj/{cnpj}?token={api_token}"

        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status == 200:
                    cnpj_data = await response.json()
                    if cnpj_data:
                        return cnpj_data
                    else:
                        print("No valid CNPJ data found") 
                        return None
                else:
                    print(f"Erro ao consultar CNPJ: {response.status} - {await response.text()}")
                    return None
    except Exception as e:
        print(f"Erro ao consultar API de CNPJ: {e}")
        return None

async def send_to_rabbitmq(companies):
    """Envia os dados das empresas para o RabbitMQ."""
    print("Enviando empresas para o RabbitMQ", companies)
    connection = setup_rabbitmq()
    channel = setup_channel(connection)
    exchange_name = 'companies_exchange'

    for company in companies:
        message = json.dumps(company)
        try:
            channel.basic_publish(
                exchange=exchange_name,
                routing_key='',
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                )
            )
            print(f"Dados da empresa enviados: {company}")
        except pika.exceptions.UnroutableError:
            print(f"Erro ao publicar a mensagem: {company}")
            continue

    connection.close()

async def process_company_data(company_data, google_id, search_city):
    """Processa os dados da empresa e adiciona as informações detalhadas."""
    cnpj = company_data.get('company_cnpj')
    if cnpj:
        cnpj_data = await fetch_cnpj_data(cnpj)
        company_data['cnpj_details'] = cnpj_data
        if cnpj_data:
            company_data['cnpj_details'] = cnpj_data
            print("cnpj_data", cnpj_data)
            print("cnpj_data",cnpj_data)
            company_city_cleaned = re.sub(r'/.*', '', company_data.get('company_city', '')).strip()
            search_city_cleaned = search_city.strip()

        if company_data['company_status'] == "ATIVA" and company_city_cleaned == search_city_cleaned:
            return company_data
    return None

async def parse_company_data(html_data, google_id, search_city):
    """Analisa os dados HTML retornados pela API e extrai as informações de todas as empresas."""
    try:
        
        if html_data is not None:
            soup = BeautifulSoup(html_data, 'html.parser')
            li_tags = soup.find_all('li')

            companies = []
            tasks = []

            for li_tag in li_tags:
                company_name_tag = li_tag.find('p', class_=re.compile(r'text-lg'))
                company_name = company_name_tag.get_text(strip=True) if company_name_tag else None

                company_status = None
                if "ATIVA" in li_tag.get_text():
                    company_status = "ATIVA"
                elif "BAIXADA" in li_tag.get_text():
                    company_status = "BAIXADA"
                elif "INAPTA" in li_tag.get_text():
                    company_status = "INAPTA"

                a_tag = li_tag.find('a', href=True)
                company_cnpj = re.search(r'\d{14}', a_tag['href']).group(0) if a_tag and re.search(r'\d{14}', a_tag['href']) else None

                location_tag = li_tag.find('svg', {'use': re.compile(r'#location')})
                company_city = None
                if location_tag:
                    city_tag = location_tag.find_parent('p')
                    if city_tag:
                        for svg in city_tag.find_all('svg'):
                            svg.extract()
                        company_city = city_tag.get_text(strip=True).replace('\n', '').strip()

                company_data = {
                    'company_name': company_name,
                    'company_cnpj': company_cnpj,
                    'company_city': company_city,
                    'company_status': company_status,
                    'google_id': google_id
                }

                # Adicionando a tarefa para processar a empresa
                task = asyncio.create_task(process_company_data(company_data, google_id, search_city))
                tasks.append(task)

            # Aguardando todas as tarefas serem concluídas
            results = await asyncio.gather(*tasks)

            # Filtrando empresas válidas (não nulas)
            valid_companies = [company for company in results if company]

            return valid_companies
        else:
            print("No HTML data found")  # Added check for invalid HTML data
            return []
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return []

async def handle_lead_data(lead_data):
    """Processa os dados de lead e busca as informações da empresa."""
    name = lead_data.get('Name')
    city = lead_data.get('City')
    google_id = lead_data.get('PlaceID')

    if name and city:
        try:
            
            api_key = os.getenv('RAPIDAPI_KEY')
            search_url = f"https://cnpj.biz/procura/{quote(name)}%20{quote(city)}"
            response_data = await fetch_data_from_api(api_key, search_url)
            companies_info = await parse_company_data(response_data, google_id, city)

            if companies_info:
                # 2. Chama a API Google Serper, utilizando as informações obtidas da consulta anterior
                print(f"Chamando a API Google Serper para {name}, {city}")
                serper_response = await fetch_serper_data(name, city)
                print(f"Resposta da API Google Serper: {serper_response}")

                # 3. Combine dados do Serper, do CNPJ.biz e os detalhes de CNPJ em uma única estrutura
                for company in companies_info:
                    # Obtém detalhes completos do CNPJ
                    cnpj = company.get('company_cnpj')
                    if cnpj:
                        cnpj_details = await fetch_cnpj_data(cnpj)
                        company['cnpj_details'] = cnpj_details

                combined_data = {
                    'serper_data': json.loads(serper_response),  # Dados da API Google Serper
                    'companies_info': companies_info  # Informações da API de CNPJ com detalhes de CNPJ
                }

                # Imprime a estrutura combinada
                print("Dados combinados de todas as APIs:", json.dumps(combined_data, indent=4, ensure_ascii=False))

                # 4. Envia os dados para o RabbitMQ
                await send_to_rabbitmq(companies_info)
            else:
                print("Nenhuma informação de empresa encontrada.")
        except Exception as e:
            print(f"Erro ao buscar dados da API: {str(e)}")

def main():
    """Função principal para iniciar o serviço de scraping de leads."""
    connection = setup_rabbitmq()
    channel = setup_channel(connection)

    print(" [*] Esperando por mensagens. Para sair pressione CTRL+C")

    def callback(ch, method, properties, body):
        """Callback para processar mensagens da fila."""
        lead_data = json.loads(body)
        asyncio.run(handle_lead_data(lead_data))

    channel.basic_consume(queue='scrapper_queue', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()
