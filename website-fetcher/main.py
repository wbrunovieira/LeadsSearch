import json
import os
import re

# pylint: disable=E0401
from langdetect import detect, LangDetectException
import aiohttp
import requests
import pika
from bs4 import BeautifulSoup
from redis import Redis
from urllib.parse import quote
from dotenv import load_dotenv
from wappalyzer import Wappalyzer, WebPage
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
    
    
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)
    channel.queue_declare(queue='leads_queue', durable=True)
    

    return channel


def fetch_data_from_url(url,max_pages=10):
    """Faz scraping da página inicial e segue links, extraindo informações úteis para abordagem comercial."""
    
    session = requests.Session()
     session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    visited_urls = set()
    page_count = 0

    def scrape_page(current_url):
        """Função recursiva para visitar páginas e extrair informações."""
        nonlocal page_count
        if current_url in visited_urls or not current_url.startswith(url) or page_count >= max_pages:
            return

        try:
            response = session.get(current_url, timeout=10)
            visited_urls.add(current_url)
            page_count += 1

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                logging.info(f"Extraindo dados da página: {current_url}")
                
                
                html_content = response.text

               
                metatags = extract_metatags(soup)
                pixels = extract_tracking_pixels(html_content)
                technologies = identify_technologies(current_url)
                page_title = extract_page_title(soup)
                social_links = extract_social_links(soup, current_url)
                contact_info = extract_contact_info(soup, html_content)

               
                print(f"URL: {current_url}")
                print("Título da Página:", page_title)
                print("Metatags:", metatags)
                print("Pixels:", pixels)
                print("Tecnologias detectadas:", technologies)
                print("Links para Redes Sociais:", social_links)
                print("Informações de Contato:", contact_info)
                print("HTML tamanho:", len(html_content))

                
                links = extract_links(soup, current_url)
                for link in links:
                    scrape_page(link)

            else:
                logging.warning(f"Falha ao acessar {current_url}: Status {response.status_code}")

        except requests.RequestException as e:
            logging.error(f"Erro ao fazer scraping de {current_url}: {e}")

    
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
    phone_numbers = re.findall(r'\(?\b[0-9]{2,4}\)?[-.\s]?[0-9]{4,5}[-.\s]?[0-9]{4}\b', html_content)
    if phone_numbers:
        contact_info['Telefones'] = phone_numbers

    # Tentar encontrar emails
    emails = re.findall(r'[\w\.-]+@[\w\.-]+', html_content)
    if emails:
        contact_info['Emails'] = emails

    # Procurar endereço (pode ser mais complexo, mas aqui é uma abordagem básica)
    address = soup.find(text=re.compile(r'\d{1,3}\s\w+\s\w+'))  # Exemplo básico
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


def identify_technologies(url):
    """Usa Wappalyzer para detectar as tecnologias usadas no site."""
    try:
        webpage = WebPage.new_from_url(url)
        wappalyzer = Wappalyzer.latest()
        technologies = wappalyzer.analyze(webpage)
        return technologies
    except Exception as e:
        logging.error(f"Erro ao identificar tecnologias para {url}: {e}")
        return []


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


def consume_leads_from_rabbitmq(channel):
    """Função para consumir leads do RabbitMQ e processar os dados recebidos."""
    exchange_name = "leads_exchange"
    queue_name = "leads_queue"

   
    try:
        channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='fanout',
            durable=True
        )
    except Exception as e:
        logging.error(f"Falha ao declarar exchange: {e}")
        return

    
    try:
        channel.queue_declare(queue=queue_name, durable=True)
    except Exception as e:
        logging.error(f"Falha ao declarar fila: {e}")
        return

    
    try:
        channel.queue_bind(
            queue=queue_name,
            exchange=exchange_name
        )
    except Exception as e:
        logging.error(f"Falha ao vincular fila à exchange: {e}")
        return

    def callback(ch, method, properties, body):
        """Callback para processar mensagens da fila."""
        try:
           
            if not is_valid_json(body):
                logging.warning("Mensagem recebida não é JSON válido")
                return

            logging.info(f"Mensagem recebida do RabbitMQ: {body.decode()}")
            lead_data = json.loads(body)

            
            save_lead_to_database(lead_data)

        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar JSON: {e}")
        except Exception as e:
            logging.error(f"Erro ao processar a mensagem: {e}")

    
    try:
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )
        logging.info("Consumindo leads do RabbitMQ...")
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Erro ao consumir mensagens do RabbitMQ: {e}")


async def process_lead_data(lead_data, redis_client):
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