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
      "q": f"{name}, {city}", 
      "gl": "br",  
      "hl": "pt-br", 
      "num": 30  
    })
    
    headers = {
        'X-API-KEY': os.getenv('SERPER_API_KEY'), 
        'Content-Type': 'application/json'
    }

    conn.request("POST", "/search", payload, headers)
    res = conn.getresponse()
    data = res.read()

    if res.status != 200:
            print(f"[LOG] Erro na API Serper. Status: {res.status}, Motivo: {res.reason}")
            return None
        
    print(f"[LOG] Dados recebidos da API Serper: {data.decode('utf-8')}")
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
            print(f"[LOG] Status da resposta: {response.status}")
            if response.status == 200:
                    
                    response_text = await response.text()
                    print(f"[LOG] Dados recebidos com sucesso da API: {response_text[:500]}...")  
                    return response_text
            else:
                    
                    print(f"[LOG] Erro na solicitação: Status {response.status}, motivo: {response.reason}")
                    return None
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

def send_to_rabbitmq(combined_data):
    """Envia os dados das empresas para o RabbitMQ."""
    print("Enviando dados combinados para o RabbitMQ", combined_data)
    connection = setup_rabbitmq()
    channel = setup_channel(connection)
    exchange_name = 'companies_exchange'

    try:
        
        message = json.dumps(combined_data)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key='',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  
            )
        )
        print(f"Dados enviados para o RabbitMQ: {combined_data}")
    except pika.exceptions.UnroutableError as e:
        print(f"Erro ao publicar a mensagem: {e}")
    finally:
        
        connection.close()
        print("Conexão com o RabbitMQ fechada.")

def format_city_name(city_name):
   
    city_name = re.sub(r'/.*', '', city_name).strip()
    
    
    prepositions = [
        'da', 'de', 'do', 'das', 'dos', 'e',      
        'of', 'the', 'and', 'in', 'on',           
        'di', 'del', 'della', 'dei', 'da', 'e',   
        'de', 'del', 'la', 'las', 'y'             
    ]

   
    words = city_name.split()
    formatted_words = [
        word.capitalize() if word.lower() not in prepositions else word.lower()
        for word in words
    ]
    
    
    return ' '.join(formatted_words)

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
                print(f"achou o status company_status{company_status}")
                a_tag = li_tag.find('a', href=True)
                company_cnpj = re.search(r'\d{14}', a_tag['href']).group(0) if a_tag and re.search(r'\d{14}', a_tag['href']) else None

                # Tentativa 1: Usar o ícone de localização SVG (já existente no código)
                location_tag = li_tag.find('svg', {'use': re.compile(r'#location')})
                company_city = None
                if location_tag:
                    city_tag = location_tag.find_parent('p')
                    if city_tag:
                        for svg in city_tag.find_all('svg'):
                            svg.extract()  # Remove SVGs
                        company_city = city_tag.get_text(strip=True).replace('\n', '').strip()
                        print(f"[LOG] Tentativa 1: Cidade encontrada: {company_city}")
                
                # Tentativa 2: Verificar se a cidade pode estar dentro de outra div com atributos diferentes
                if not company_city:
                    possible_city_tags = li_tag.find_all('p', class_=re.compile(r'text-sm'))
                    for possible_city_tag in possible_city_tags:
                        if re.search(r'\b\w+/\w{2}\b', possible_city_tag.get_text()):  # Busca por formato Cidade/UF
                            company_city = possible_city_tag.get_text(strip=True)
                            print(f"[LOG] Tentativa 2: Cidade encontrada: {company_city}")
                            break

                # Tentativa 3: Usar regex no conteúdo de texto total, caso a cidade esteja fora de tags específicas
                if not company_city:
                    full_text = li_tag.get_text(separator=" ", strip=True)
                    match = re.search(r'\b\w+/\w{2}\b', full_text)
                    if match:
                        company_city = match.group(0)
                        print(f"[LOG] Tentativa 3: Cidade encontrada via regex: {company_city}")

                # Tentativa 4: Outro método baseado em estrutura específica
                if not company_city:
                    print(f"[LOG] Cidade não encontrada. HTML do bloco: {li_tag}")
                

                print(f"achou no parse da pagina company_name:{company_name}, company_cnpj:{company_cnpj}, company_city:{company_city}, company_status:{company_status}, google_id:{google_id}")
                
                print(f"achou no parse da pagina company_name:{company_name},company_cnpj:{company_cnpj},company_cnpj:{company_cnpj},company_city:{company_city},company_status:{company_status},google_id:{google_id}")
                company_data = {
                    'company_name': company_name,
                    'company_cnpj': company_cnpj,
                    'company_city': company_city,
                    'company_status': company_status,
                    'google_id': google_id
                }
                print(f"parceado dados do cnpj biz: {company_data} ")

                
                company_city_cleaned = format_city_name(company_city) if company_city else None
                search_city_cleaned = format_city_name(search_city)
                print("company_city_cleaned",company_city_cleaned)
                print("search_city_cleaned",search_city_cleaned)
                if company_status == "ATIVA" and company_city_cleaned == search_city_cleaned:
                    company_data = {
                        'company_name': company_name,
                        'company_cnpj': company_cnpj,
                        'company_city': company_city,
                        'company_status': company_status,
                        'google_id': google_id,
                        
                    }
                    print("company_data esse e o retorno dentro da parse_company_data",company_data)

                    companies.append(company_data)

            return companies
        else:
            print("[LOG] No HTML data found")
            return []
    except Exception as e:
        print(f"[LOG] Ocorreu um erro: {e}")
        return []    


async def handle_lead_data(lead_data):
    """Processa os dados de lead e busca as informações da empresa."""
    name = lead_data.get('Name')
    city = lead_data.get('City')
    google_id = lead_data.get('PlaceID')
    print(f"entrou no handle_lead_data com os dados:{name} {city} {google_id}")

    if name and city:
        try:            
            api_key = os.getenv('RAPIDAPI_KEY')
            print(f"[LOG] para montar url da cnpj.biz Nome: {name}, Cidade: {city}")

            search_url = f"https://cnpj.biz/procura/{quote(name)}%20{quote(city)}"
            print(f"url para consultar na cnpj.biz: {search_url}")
            response_data = await fetch_data_from_api(api_key, search_url)
            print(f"consultou cnpj.biz com {name} {city} ")
            
            
            companies_info = await parse_company_data(response_data, google_id, city)
            print(f"foi no parse para encontrar as informacoes da empresa e retornou: {companies_info} ")

            combined_data = {
                'serper_data': None,
                'companies_info': companies_info,
                'cnpj_details': []
            }
            if companies_info:
                for company in companies_info:
                    cnpj = company.get('company_cnpj')
                    print("dentro do handle achou o cnpj do companies_info:",cnpj)
                    if cnpj:
                        print(f"[LOG] Consultando API de CNPJ para {company['company_name']} com CNPJ: {cnpj}")
                        cnpj_details = await fetch_cnpj_data(cnpj) 

                        if cnpj_details not in combined_data['cnpj_details']:
                            company['cnpj_details'] = cnpj_details
                            combined_data['cnpj_details'].append(cnpj_details)
                            print(f"[LOG] Detalhes do CNPJ adicionados: {cnpj_details}")
                        
                    else:
                        print(f"[LOG] Detalhes do CNPJ não encontrados para { ['company_name']}")


            
                # 2. Chama a API Google Serper, utilizando as informações obtidas da consulta anterior
                
                print(f"Chamando a API Google Serper para {name}, {city}")
                serper_response = await fetch_serper_data(name, city)
                print(f"Resposta da API Google Serper: {serper_response}")

                if serper_response:
                    print(f"[LOG] Resposta da API Serper recebida com sucesso para {name}, {city}")
                    combined_data['serper_data'] = json.loads(serper_response)
                else:
                    print(f"[LOG] Resposta da API Serper veio vazia para {name}, {city}")

               

                # Imprime a estrutura combinada
                print("Dados combinados de todas as APIs:", json.dumps(combined_data, indent=4, ensure_ascii=False))

                # 4. Envia os dados para o RabbitMQ
                send_to_rabbitmq(combined_data)
            else:
                print("Nenhuma informação de empresa encontrada da companies_info.",{companies_info})
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
