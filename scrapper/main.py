import os
import time
import json
import re

import random
# pylint: disable=E0401
import pika
import requests
import http.client 
from html import unescape

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
    sanitized_str = input_str.strip()  
    sanitized_str = ' '.join(sanitized_str.split())  
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

    

    return data.decode("utf-8")

def parse_company_data(html_data):
    """Analisa os dados HTML retornados pela API e extrai as informações de todas as empresas."""
    try:
        soup = BeautifulSoup(html_data, 'html.parser')

        
        companies = []

        
        li_tags = soup.find_all('li')

        for li_tag in li_tags:
            
            company_name_tag = li_tag.find('p', class_=re.compile(r'text-lg'))
            company_name = company_name_tag.get_text(strip=True) if company_name_tag else None

            if not company_name:
                print("Nome da empresa não encontrado na tag <li>:")
                print(li_tag.prettify())  

            
            company_status = None
            if "ATIVA" in li_tag.get_text():
                company_status = "ATIVA"
                print(f"Status 'ATIVA' encontrado na tag: {li_tag.prettify()}")
            elif "BAIXADA" in li_tag.get_text():
                company_status = "BAIXADA"
                print(f"Status 'BAIXADA' encontrado na tag: {li_tag.prettify()}")
            elif "INAPTA" in li_tag.get_text():
                company_status = "INAPTA"
                print(f"Status 'INAPTA' encontrado na tag: {li_tag.prettify()}")

            if not company_status:
                print("Status não encontrado na tag <li> ou status desconhecido:")
                print(li_tag.prettify())

            
            a_tag = li_tag.find('a', href=True)
            company_cnpj = None
            if a_tag:
                company_cnpj = re.search(r'\d{14}', a_tag['href']).group(0) if re.search(r'\d{14}', a_tag['href']) else None
            if not company_cnpj:
                print("CNPJ não encontrado na tag <a> ou URL inválida:")
                print(a_tag.prettify() if a_tag else "Tag <a> não encontrada")
            
            
            company_city = None
            location_tag = li_tag.find('svg', {'use': re.compile(r'#location')})
            
            
            if location_tag:
                city_tag = location_tag.find_parent('p')
                if city_tag:
                    
                    for svg in city_tag.find_all('svg'):
                        svg.extract()
                    city_text = city_tag.get_text(strip=True)
                    company_city = city_text.replace('\n', '').strip()
            
            
            if not company_city:
                potential_city_tags = li_tag.find_all('p', class_=re.compile(r'text-gray-500'))
                for tag in potential_city_tags:
                    if re.search(r'[A-Za-z]+/[A-Za-z]{2}', tag.get_text()):
                        company_city = tag.get_text(strip=True)
                        break  
            
            
            if not company_city and location_tag:
                next_sibling = location_tag.find_next_sibling(text=True)
                if next_sibling and re.search(r'[A-Za-z]+/[A-Za-z]{2}', next_sibling.strip()):
                    company_city = next_sibling.strip()

            
            if not company_city:
                all_p_tags = li_tag.find_all('p')
                for tag in all_p_tags:
                    if re.search(r'[A-Za-z]+/[A-Za-z]{2}', tag.get_text()):
                        company_city = tag.get_text(strip=True)
                        break
            
            if not company_city:
                print("Cidade não encontrada para a empresa:")
                print(li_tag.prettify())  

            print("company_name",company_name)
            print("company_status",company_status)
            print("company_cnpj",company_cnpj)
            print("company_city",company_city)

            
            if company_name and company_city:
                company_data = {
                    'company_name': company_name,
                    'company_cnpj': company_cnpj,
                    'company_city': company_city,
                    'company_status': company_status
                }
                companies.append(company_data)

        return companies

    except Exception as e:
        print(f"Ocorreu um erro: {e}")


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
            
            
            api_key = os.getenv('RAPIDAPI_KEY')
            search_url = f"https://cnpj.biz/procura/{quote(sanitized_name)}%20{quote(sanitized_city)}"
            
            print(f"Buscando na API a URL: {search_url}")
            try:
                response_data = fetch_data_from_api(api_key, search_url)
                companies_info = parse_company_data(response_data)
            except Exception as e:
                print(f"Erro ao buscar dados da API: {str(e)}")
                companies_info = []
            
            if companies_info:
                print(f"Detalhes das Empresas: {json.dumps(companies_info, indent=4, ensure_ascii=False)}")
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
