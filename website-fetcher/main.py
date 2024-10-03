import json
import os
import re
from urllib.parse import urljoin


# pylint: disable=E0401
from langdetect import detect, LangDetectException
import aiohttp
import requests
import pika
from bs4 import BeautifulSoup
from redis import Redis
from urllib.parse import urljoin, urlparse
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
    
    
    redis_client = Redis(host=redis_host, port=redis_port, db=0)

    try:
       
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
    exchange_name = 'leads_exchange'
    queue_name = 'leads_queue_lead_search_to_website_fetcher'
    

    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

    try:
        channel.queue_delete(queue=queue_name)
    except pika.exceptions.ChannelClosedByBroker as e:
        logging.warning("Queue %s does not exist or could not be deleted. Proceeding to declare. erro:%s",queue_name,e)

        args = {
            'x-message-ttl': 60000, # TTL de 60 segundos
            'x-dead-letter-exchange': 'dlx_exchange'  # Dead-letter exchange
        }

        channel.queue_declare(queue=queue_name, durable=True, arguments=args)
        channel.queue_bind(queue=queue_name, exchange=exchange_name)

    return channel


def fetch_data_from_url(url,max_pages=10):
    """Faz scraping da página inicial e segue links, extraindo informações úteis para abordagem comercial."""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    visited_urls = set()
    page_count = 0
    base_domain = urlparse(url).netloc

    def scrape_page(current_url):
        """Função recursiva para visitar páginas e extrair informações."""
        nonlocal page_count
        current_domain = urlparse(current_url).netloc
        if current_url in visited_urls or base_domain not in current_domain or page_count >= max_pages:
            return

        try:
            response = session.get(current_url, timeout=10)
            visited_urls.add(current_url)
            print("pagina atual",page_count)
            page_count += 1

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                logging.info("Extraindo dados da página: %s",current_url)
                                
                html_content = response.text
                plain_text = soup.get_text()

               
                metatags = extract_metatags(soup)
                pixels = extract_tracking_pixels(html_content)
                
                page_title = extract_page_title(soup)
                social_links = extract_social_links(soup, current_url)
                contact_info = extract_contact_info(soup, html_content)

                clean_text = ' '.join(plain_text.split())

                print("URL: %s, current_url")
               

                print("Título da Página:", page_title)
                print("texto da pagina", clean_text)
                print("Metatags:", metatags)
                print("Pixels:", pixels)
                
                print("Links para Redes Sociais:", social_links)
                print("Informações de Contato:", contact_info)
                print("HTML tamanho:", len(html_content))

                
                links = extract_links(soup, current_url)
                for link in links:
                    scrape_page(link)

            else:
                logging.warning("Falha ao acessar %s: Status %s ",current_url,response.status_code)

        except requests.RequestException as e:
            logging.error("Erro ao fazer scraping de  %s:  %s",current_url,e)

    
    scrape_page(url)


def extract_metatags(soup):
    """Extrai todas as metatags de uma página."""
    metatags = {}
    for tag in soup.find_all('meta'):
        if 'name' in tag.attrs and 'content' in tag.attrs:
            metatags[tag['name']] = tag['content']
        elif 'property' in tag.attrs and 'content' in tag.attrs:
            metatags[tag['property']] = tag['content']
    return metatags


def extract_page_title(soup):
    """Extrai o título da página."""
    return soup.title.string if soup.title else "Sem título"


def extract_tracking_pixels(html_content):
    """Extrai pixels de rastreamento do HTML da página."""
    pixels = {}

    # Verificar presença de pixel do Facebook
    facebook_pixel = re.findall(r'fbq\(.+?\)', html_content)
    if facebook_pixel:
        pixels['Facebook Pixel'] = facebook_pixel

    # Verificar presença de pixel do Google Analytics
    google_analytics = re.findall(r'gtag\(.+?\)|ga\(.+?\)', html_content)
    if google_analytics:
        pixels['Google Analytics'] = google_analytics

    return pixels


def extract_social_links(soup, base_url):
    """Extrai links para redes sociais da página."""
    social_links = {}
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if "facebook.com" in href:
            social_links['Facebook'] = urljoin(base_url, href)
        elif "instagram.com" in href:
            social_links['Instagram'] = urljoin(base_url, href)
        elif "linkedin.com" in href:
            social_links['LinkedIn'] = urljoin(base_url, href)
    return social_links


def extract_contact_info(soup, html_content):
    """Extrai informações de contato, como telefone, email e endereço."""
    contact_info = {}

    # Tentar encontrar telefones
    phone_numbers = re.findall(r'(\+55\s?)?\(?\d{2}\)?[-.\s]?\d{4,5}[-.\s]?\d{4}', html_content)
    if phone_numbers:
        contact_info['Telefones'] = phone_numbers

    # Tentar encontrar emails
    emails = re.findall(r'[\w\.-]+@[\w\.-]+', html_content)
    if emails:
        contact_info['Emails'] = emails

    address_keywords = ['Rua', 'Avenida', 'Travessa', 'Estrada', 'Praça']
    address = None
    for keyword in address_keywords:
        address_candidate = soup.find(text=re.compile(rf'{keyword}\s+\w+', re.IGNORECASE))
        if address_candidate:
            address = address_candidate.strip()
            break

    if address:
        contact_info['Endereço'] = address

    return contact_info


def extract_links(soup, base_url):
    """Extrai todos os links de uma página e os converte para URLs completas."""
    links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        full_url = urljoin(base_url, href)
        if urlparse(full_url).netloc == urlparse(base_url).netloc:  # Limita ao mesmo domínio
            links.add(full_url)
    return links


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


def consume_leads_from_rabbitmq(channel,redis_client):
    """Função para consumir leads do RabbitMQ e processar os dados recebidos."""
    exchange_name = "leads_exchange"
    queue_name = "leads_queue_lead_search_to_website_fetcher"

   
    try:
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='fanout',
            durable=True
        )
    except Exception as e:
        logging.error("Falha ao declarar exchange: %s",e)
        return

    
    try:
        channel.queue_declare(queue=queue_name, durable=True)
    except Exception as e:
        logging.error("Falha ao declarar fila: %s",e)
        return

    
    try:
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name
        )
    except Exception as e:
        logging.error("Falha ao vincular fila à exchange: %s",e)
        return

    try:
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body: callback(ch, method, properties, body, redis_client),
            auto_ack=False 
        )
        logging.info("Consumindo leads do RabbitMQ...")
        channel.start_consuming()
    except Exception as e:
        logging.error("Erro ao consumir mensagens do RabbitMQ: %s",e)

    def callback(ch, method, properties, body,redis_client):
        """Callback para processar mensagens da fila."""
        try:
           
            if not is_valid_json(body):
                logging.warning("Mensagem recebida não é JSON válido")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            logging.info("Mensagem recebida do RabbitMQ: %s",{body.decode()})
            lead_data = json.loads(body)

            
            process_lead_data(lead_data,redis_client)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logging.error("Erro ao decodificar JSON:  %s",e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logging.error("Erro ao processar a mensagem:  %s",e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)



def process_lead_data(lead_data, redis_client):
    lead_id = get_lead_id_from_redis(redis_client, lead_data.get("PlaceID"))
    if not lead_id:
        logging.warning("Lead ID não encontrado para o Google ID %s", lead_data.get("PlaceID"))
        return

    website = lead_data.get("Website", "")
    if website:
        if website.startswith("https://www.instagram.com"):
            logging.info("Instagram URL detectada, ignorando scraping.")
        elif website.startswith("https://www.facebook.com"):
            logging.info("Facebook URL detectada, ignorando scraping.")
        else:
            fetch_data_from_url(website)

def is_valid_json(data):
    """Verifica se o corpo da mensagem é um JSON válido."""
    try:
        json.loads(data)
        return True
    except ValueError:
        return False

def main():
    
    redis_client = setup_redis()
    if not redis_client:
        print("Encerrando o serviço devido à falha de conexão com o Redis.")
        return

    connection = setup_rabbitmq()
    channel = setup_channel(connection)

    consume_leads_from_rabbitmq(channel, redis_client)
    
    channel.start_consuming()
    
    print(" [*] Esperando por mensagens no website-fetcher. Para sair pressione CTRL+C")

if __name__ == "__main__":
    main()