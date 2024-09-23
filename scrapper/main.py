import pika
import os
import json
import time
from bs4 import BeautifulSoup
import requests

def main():
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port)
    )
    channel = connection.channel()

    exchange_name = 'leads_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

    queue_name = 'scrapper_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    print(" [*] Esperando por mensagens. Para sair pressione CTRL+C")

    def parse_search_results(html_content, searched_city):
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

    def parse_company_details(html_content):
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
        try:
            lead_data = json.loads(body)
            print(f" [x] Recebido Lead para Scrapper: {json.dumps(lead_data, indent=4, ensure_ascii=False)}")

            name = lead_data.get('Name')
            city = lead_data.get('City')
            if name and city:
                search_url = f"https://cnpj.biz/procura/{name} {city}"
                response = requests.get(search_url)
                if response.status_code == 200:
                    html_content = response.text
                    search_results = parse_search_results(html_content, city)
                    for result in search_results:
                        cnpj = result['cnpj']
                        company_url = f"https://cnpj.biz/{cnpj}"
                        company_response = requests.get(company_url)
                        if company_response.status_code == 200:
                            company_html = company_response.text
                            company_details = parse_company_details(company_html)
                            print(f"Detalhes da Empresa: {company_details}")
                        else:
                            print(f"Falha ao acessar dados da empresa com CNPJ {cnpj}")
                else:
                    print(f"Falha ao buscar dados de {name} em {city}")
            else:
                print("Nome ou Cidade não encontrados no lead.")
        except json.JSONDecodeError as e:
            print(f" [!] Erro ao decodificar JSON: {e}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(" [x] Interrompido pelo usuário")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == "__main__":
    main()
