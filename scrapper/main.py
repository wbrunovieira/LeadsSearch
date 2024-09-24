import os
import time
import json
import random
# pylint: disable=E0401
import pika
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
from urllib.parse import quote

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

def setup_selenium():
    """Configura o Selenium WebDriver."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--remote-debugging-port=9222')
    
    driver = webdriver.Chrome(options=options)
    return driver

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

def fetch_url_with_selenium(driver, url):
    """Busca URL usando Selenium WebDriver e retorna o conteúdo HTML."""
    driver.get(url)
    time.sleep(random.uniform(1, 2))  # Aguarda o carregamento da página
    return driver.page_source



def parse_company_details(driver):
    """Extrai detalhes da empresa diretamente com Selenium."""
    company_info = {}
    
    try:
        # Extrair o nome completo da empresa
        name_element = driver.find_element(By.CSS_SELECTOR, 'h1.post-title.empresa-title')
        company_info['full_name'] = name_element.text.strip()

        # Extrair o CNPJ
        cnpj_element = driver.find_element(By.XPATH, "//span[contains(text(), 'CNPJ:')]")
        cnpj_value = cnpj_element.find_element(By.XPATH, "following-sibling::b").text.strip()
        company_info['cnpj'] = cnpj_value

    except Exception as e:
        print(f"Erro ao extrair detalhes da empresa: {e}")

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
            
            # Configura o Selenium WebDriver
            driver = setup_selenium()

            search_url = f"https://cnpj.biz/procura/{quote(name)}%20{quote(city)}"
            print("search_url", search_url)

            html_content = fetch_url_with_selenium(driver, search_url)
            search_results = parse_search_results(html_content, city)

            for result in search_results:
                cnpj = result['cnpj']
                company_url = f"https://cnpj.biz/{cnpj}"
                print("company_url", company_url)

                company_html_content = fetch_url_with_selenium(driver, company_url)
                company_details = parse_company_details(company_html_content)
                print(f"Detalhes da Empresa: {company_details}")
            
            # Fecha o driver após o processamento
            driver.quit()

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
