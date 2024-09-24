import os
import time
import json
import random
from urllib.parse import quote
# pylint: disable=E0401
import pika
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import cloudscraper
import requests

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

def parse_search_results(html_content, searched_city):
    """Analisa os resultados de busca e filtra por cidade."""
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    for company in soup.find_all('div', class_='well search-result'):
        name_element = company.find('a', class_='lnk')
        city_element = company.find('span', class_='text-muted')
        if name_element and city_element:
            company_name = name_element.text.strip()
            company_city = city_element.text.strip().split('/')[0]
            if company_city.lower() == searched_city.lower():
                status_element = company.find('span', class_='label label-success')
                company_status = status_element.text.strip() if status_element else 'Inativa'
                if company_status.lower() == 'ativa':
                    cnpj = name_element['href'].split('/')[-1]
                    results.append({'name': company_name, 'cnpj': cnpj})
    return results

def fetch_url_with_proxy(url, headers, proxies_list):
    """Busca URL usando proxies e retorna a resposta."""
    max_retries = 5
    for _ in range(max_retries):
        proxy = random.choice(proxies_list)
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url, headers=headers, proxies=proxy, verify=False)
            if response.status_code == 200:
                return response
            else:
                print(f"Erro {response.status_code}, tentando outro proxy...")
        except requests.exceptions.RequestException as e:
            print(f"Erro ao conectar com o proxy {proxy}, tentando outro proxy... {e}")
    raise Exception("Todas as tentativas de proxy falharam")

def parse_company_details(html_content):
    """Analisa detalhes da empresa a partir do HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    company_info = {}
    name_element = soup.find('h1', class_='post-title empresa-title')
    if name_element:
        company_info['full_name'] = name_element.text.strip()
    cnpj_element = soup.find('span', text='CNPJ:')
    if cnpj_element:
        cnpj_value = cnpj_element.find_next('b').text.strip()
        company_info['cnpj'] = cnpj_value
    return company_info

def callback(ch, method, properties, body):
    """Processa mensagens da fila RabbitMQ e realiza buscas."""
    try:
        lead_data = json.loads(body)
        print(f"Lead Data: {json.dumps(lead_data, indent=4, ensure_ascii=False)}")

        name = lead_data.get('Name')
        city = lead_data.get('City')
        if isinstance(name, str) and isinstance(city, str) and name.strip() and city.strip():
            print("Name para busca", name)
            print("City para busca", city)
            time.sleep(random.uniform(1, 2))
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://cnpj.biz/',
            }
            proxies_list = [
    {'http': 'http://64.92.82.59:8080', 'https': 'http://64.92.82.59:8080'},
    {'http': 'http://72.10.160.171:10095', 'https': 'http://72.10.160.171:10095'},
    {'http': 'http://72.10.164.178:1417', 'https': 'http://72.10.164.178:1417'},
    {'http': 'http://67.43.227.228:23737', 'https': 'http://67.43.227.228:23737'},
    {'http': 'http://160.86.242.23:8080', 'https': 'http://160.86.242.23:8080'},
    {'http': 'http://37.187.25.85:80', 'https': 'http://37.187.25.85:80'},
    {'http': 'http://52.67.10.183:80', 'https': 'http://52.67.10.183:80'},
    {'http': 'http://13.38.153.36:80', 'https': 'http://13.38.153.36:80'},
    {'http': 'http://18.228.149.161:80', 'https': 'http://18.228.149.161:80'},
    {'http': 'http://3.212.148.199:80', 'https': 'http://3.212.148.199:80'},
    {'http': 'http://13.37.59.99:80', 'https': 'http://13.37.59.99:80'},
    {'http': 'http://52.196.1.182:80', 'https': 'http://52.196.1.182:80'},
    {'http': 'http://43.202.154.212:80', 'https': 'http://43.202.154.212:80'},
    {'http': 'http://13.208.56.180:80', 'https': 'http://13.208.56.180:80'},
    {'http': 'http://35.72.118.126:80', 'https': 'http://35.72.118.126:80'},
    {'http': 'http://13.37.73.214:80', 'https': 'http://13.37.73.214:80'},
    {'http': 'http://3.141.217.225:80', 'https': 'http://3.141.217.225:80'},
    {'http': 'http://3.124.133.93:80', 'https': 'http://3.124.133.93:80'},
    {'http': 'http://35.76.62.196:80', 'https': 'http://35.76.62.196:80'},
    {'http': 'http://18.228.198.164:80', 'https': 'http://18.228.198.164:80'},
    {'http': 'http://43.201.121.81:80', 'https': 'http://43.201.121.81:80'},
    {'http': 'http://3.127.121.101:80', 'https': 'http://3.127.121.101:80'},
    {'http': 'http://13.37.89.201:80', 'https': 'http://13.37.89.201:80'},
    {'http': 'http://3.71.239.218:80', 'https': 'http://3.71.239.218:80'},
    {'http': 'http://3.126.147.182:80', 'https': 'http://3.126.147.182:80'},
    {'http': 'http://3.127.62.252:80', 'https': 'http://3.127.62.252:80'},
    {'http': 'http://3.122.84.99:80', 'https': 'http://3.122.84.99:80'}
                                    ]

            search_url = f"https://cnpj.biz/procura/{quote(name)}%20{quote(city)}"
            print("search_url", search_url)

            response = fetch_url_with_proxy(search_url, headers, proxies_list)

            if response.status_code == 200:
                html_content = response.text
                search_results = parse_search_results(html_content, city)
                for result in search_results:
                    cnpj = result['cnpj']
                    company_url = f"https://cnpj.biz/{cnpj}"
                    print("company_url", company_url)
                    company_response = fetch_url_with_proxy(company_url, headers, proxies_list)
                    if company_response.status_code == 200:
                        company_soup = BeautifulSoup(company_response.text, 'html.parser')
                        print(company_soup.prettify())
                        print(f"Detalhes da Empresa: {name} e com CNPJ {cnpj}")
                    else:
                        print(f"Falha ao acessar dados da empresa com CNPJ {cnpj}")
            else:
                print(f"Falha ao buscar dados de {name} em {city}")
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
