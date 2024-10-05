import json
import http.client
import json
import time

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

allowed_languages = ['pt']


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


def fetch_data_from_url(url,max_pages=1):
    """Faz scraping da página inicial e segue links, extraindo informações úteis para abordagem comercial."""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    visited_urls = set()
    page_count = 0
    base_domain = urlparse(url).netloc
    cloudflare_pages = []

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

                detected_language = detect_language(plain_text)
                if detected_language != 'pt':
                    print("Idioma da página %s não é permitido: detected_language",current_url, detected_language)
                    return  

            
                print(f"Linguagem detectada: {detected_language}. Processando página...")


                if handle_cloudflare_protected_pages(current_url, soup, html_content, cloudflare_pages):
                    return

               
                metatags = extract_metatags(soup)
                pixels = extract_tracking_pixels(html_content)
                technologies = extract_technologies(html_content)
                print(f"Tecnologias usadas no site: {technologies}")
                
                page_title = extract_page_title(soup)
                social_links = extract_social_links(soup, current_url)
                contact_info = extract_contact_info(soup, html_content)

                clean_text = ' '.join(plain_text.split())

                print("URL da consulta: %s", current_url)
               

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
                print("links:", links)

            else:
                logging.warning("Falha ao acessar %s: Status %s ",current_url,response.status_code)

        except requests.RequestException as e:
            logging.error("Erro ao fazer scraping de  %s:  %s",current_url,e)

    
    scrape_page(url)

    if cloudflare_pages:
        print("Páginas protegidas por Cloudflare encontradas:")
        for page in cloudflare_pages:
            print(page)


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

    
    facebook_pixel = re.findall(r'fbq\(.+?\)', html_content)
    if facebook_pixel:
        pixels['Facebook Pixel'] = facebook_pixel

    
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

    
    phone_numbers = re.findall(r'\+?55?\s*\(?\b[0-9]{2,4}\)?[-.\s]?[0-9]{4,5}[-.\s]?[0-9]{4}\b', html_content)

    phone_numbers = [phone.strip() for phone in phone_numbers if phone.strip() and len(phone.strip()) > 7]

    contact_info['Telefones'] = phone_numbers if phone_numbers else None

    
    emails = re.findall(r'[\w\.-]+@[\w\.-]+', html_content)
    if emails:
        contact_info['Emails'] = emails

    address = extract_address_from_html(soup)
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

def extract_address_from_html(soup):
    """Tenta extrair o endereço da página a partir de diferentes padrões comuns em sites."""
    
    
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            json_data = json.loads(script.string)
            if isinstance(json_data, dict) and '@type' in json_data and json_data['@type'] == 'Hotel':
                address = json_data.get('address', {})
                if isinstance(address, dict):
                    street_address = address.get('streetAddress', '')
                    city = address.get('addressLocality', '')
                    postal_code = address.get('postalCode', '')
                    country = address.get('addressCountry', '')

                    full_address = f"{street_address}, {city}, {postal_code}, {country}"
                    return full_address.strip()
        except json.JSONDecodeError:
            continue  # Continua se houver erro no JSON

    # Padrão básico para tentar extrair o endereço do HTML sem JSON-LD
    # Esse padrão tenta encontrar algo semelhante a um endereço (rua, número, cidade, CEP)
    address_pattern = r"\b[0-9]{1,5}\s+\w+\s+\w+,\s+\w+,\s+[A-Z]{2},?\s+[0-9]{5}-[0-9]{3}\b"
    address_match = re.search(address_pattern, soup.get_text())
    
    if address_match:
        return address_match.group(0)

    # Caso não encontre nenhum padrão específico, retorna uma mensagem padrão
    return "Endereço não encontrado"

def extract_technologies(html_content):
    """Detecta tecnologias comuns usadas em sites, como WordPress, Magento, etc."""
    technologies = []

    # Verifica por padrões específicos de tecnologias no HTML

    # WordPress
    if re.search(r'/wp-content/', html_content):
        technologies.append('WordPress')

    # Joomla
    if re.search(r'/components/com_', html_content):
        technologies.append('Joomla')

    # Drupal
    if re.search(r'/sites/default/files/', html_content):
        technologies.append('Drupal')

    # Magento
    if re.search(r'/skin/frontend/', html_content):
        technologies.append('Magento')

    # Shopify
    if re.search(r'\.myshopify\.com', html_content) or re.search(r'/cdn.shopify.com/', html_content):
        technologies.append('Shopify')

    # Wix
    if re.search(r'wix\.com', html_content):
        technologies.append('Wix')

    # Squarespace
    if re.search(r'squarespace\.com', html_content):
        technologies.append('Squarespace')

    # Weebly
    if re.search(r'weebly\.com', html_content):
        technologies.append('Weebly')

    # Google Sites
    if re.search(r'sites.google.com', html_content):
        technologies.append('Google Sites')

    # WooCommerce (para e-commerce WordPress)
    if re.search(r'/woocommerce/', html_content):
        technologies.append('WooCommerce')

    # PrestaShop
    if re.search(r'/modules/', html_content) and re.search(r'/themes/', html_content):
        technologies.append('PrestaShop')

    # BigCommerce
    if re.search(r'/cdn.bigcommerce.com/', html_content):
        technologies.append('BigCommerce')

    # Landing page builders
    # Unbounce
    if re.search(r'unbouncepages.com', html_content):
        technologies.append('Unbounce')

    # LeadPages
    if re.search(r'myleadpages\.com', html_content):
        technologies.append('LeadPages')

    # Instapage
    if re.search(r'instapage\.com', html_content):
        technologies.append('Instapage')

    # HubSpot CMS
    if re.search(r'cdn\.hubspot\.com', html_content):
        technologies.append('HubSpot CMS')

    # ClickFunnels
    if re.search(r'clickfunnels\.com', html_content):
        technologies.append('ClickFunnels')

    # Outros frameworks e bibliotecas
    # Bootstrap
    if re.search(r'bootstrap(?:\.min)?\.css', html_content):
        technologies.append('Bootstrap')

    # FontAwesome
    if re.search(r'font-awesome(?:\.min)?\.css', html_content):
        technologies.append('FontAwesome')

    # jQuery
    if re.search(r'jquery(?:\.min)?\.js', html_content):
        technologies.append('jQuery')

    # React.js
    if re.search(r'react(?:\.min)?\.js', html_content):
        technologies.append('React.js')

    # Vue.js
    if re.search(r'vue(?:\.min)?\.js', html_content):
        technologies.append('Vue.js')

    # Angular.js
    if re.search(r'angular(?:\.min)?\.js', html_content):
        technologies.append('Angular.js')

    if not technologies:
        # Verificar se contém arquivos CSS e JS
        if re.search(r'\.css', html_content) or re.search(r'\.js', html_content):
            technologies.append('HTML, CSS, JS puro')
        else:
            technologies.append('HTML puro')

    return technologies


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


def get_lead_id_from_redis(redis_client, google_id, retries=3, delay=1):
    """Obtém o lead_id a partir do google_id no Redis com tentativas de reprocessamento."""
    redis_key = f"google_lead:{google_id}"
    for attempt in range(retries):
        try:
            lead_id = redis_client.get(redis_key)
            if lead_id:
                print(f"Lead ID {lead_id.decode('utf-8')} encontrado para o Google ID {google_id}.")
                return lead_id.decode('utf-8')
            else:
                print(f"Lead ID não encontrado no Redis para o Google ID {google_id}. Tentativa {attempt + 1}/{retries}")
                time.sleep(delay)
        except Exception as e:
            print(f"Erro ao buscar Lead ID no Redis: {e}")
            return None
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
    print("website-fetcher process_lead_data lead_id",lead_id)
    print("website-fetcher process_lead_data PlaceID",lead_data.get("PlaceID"))
    if not lead_id:
        logging.warning("Lead ID não encontrado para o Google ID %s", lead_data.get("PlaceID"))
        return

    website = lead_data.get("Website", "")
    print("website-fetcher process_lead_data website",website)
 
    
    if website:
        if website.startswith("https://www.instagram.com"):
            logging.info("Instagram URL detectada, ignorando scraping.")
        elif website.startswith("https://www.facebook.com"):
            logging.info("Facebook URL detectada, ignorando scraping.")
        else:
            fetch_data_from_url(website)
            domain_whois(website)

def format_phone_number(phone):
   
    phone = re.sub(r'\D', '', phone) 

    print('phone111',phone)
    if phone.startswith("55"):
        print('phone 2',phone)
        return phone
    else:
        
        return "55" + phone
        print('phone final',phone)


def domain_whois(domain):
    """Consulta o domínio no RDAP do registro.br ou na API do zozor54 se o domínio não terminar com .br"""
    parsed_domain = urlparse(domain).netloc
    print("aqui esta o dominio que vamos trabalhar no whois parsed_domain",parsed_domain) 
    if parsed_domain.endswith(".br"):
        
        url = f"https://rdap.registro.br/domain/{parsed_domain}"
        try:
            response = requests.get(url)
            response.raise_for_status()  
            data = response.json()

            
            extracted_data = {
                'Domain': data.get('ldhName'),
                'Registrant Organization': data['entities'][0]['vcardArray'][1][3][3] if data.get('entities') else None,
                'Registrant CNPJ': next((p['identifier'] for p in data['entities'][0]['publicIds'] if p['type'] == 'cnpj'), None),
                'Administrative Contact': data['entities'][0]['entities'][0]['vcardArray'][1][3][3] if data['entities'][0].get('entities') else None,
                'Administrative Email': data['entities'][0]['entities'][0]['vcardArray'][1][4][3] if data['entities'][0].get('entities') else None,
                'Nameservers': [ns['ldhName'] for ns in data.get('nameservers', [])],
                'Registration Date': next((e['eventDate'] for e in data.get('events', []) if e['eventAction'] == 'registration'), None),
                'Last Changed': next((e['eventDate'] for e in data.get('events', []) if e['eventAction'] == 'last changed'), None),
                'Expiration Date': next((e['eventDate'] for e in data.get('events', []) if e['eventAction'] == 'expiration'), None),
                'Status': data.get('status', [])
            }
            print("extracted_data .br",extracted_data)
            
            return extracted_data
        
        except requests.exceptions.RequestException as e:
            print(f"Erro ao consultar o domínio {parsed_domain}: {e}")
            return None
    
    else:
       
        try:

            url = "https://zozor54-whois-lookup-v1.p.rapidapi.com/"

            
            querystring = {"domain": parsed_domain, "format": "json", "_forceRefresh": "0"}


            headers = {
                'x-rapidapi-key': os.getenv('RAPIDAPI_KEY_LOOKUP'), 
                "x-rapidapi-host": "zozor54-whois-lookup-v1.p.rapidapi.com"
            }

            response = requests.get(url, headers=headers, params=querystring)

            if response.status_code != 200:
                print(f"Erro ao consultar o domínio {parsed_domain}: Código de status {response.status_code}")
                return None

            whois_data = response.json()
            print("whoisdata do int",whois_data)

            extracted_data = {
                'Domain': whois_data.get('name'),
                'Nameservers': whois_data.get('nameserver', []),
                'Created': whois_data.get('created'),
                'Changed': whois_data.get('changed'),
                'Expires': whois_data.get('expires'),
                'Registrar': whois_data.get('registrar', {}).get('name'),
                'Registrar Email': whois_data.get('registrar', {}).get('email'),
                'Registrar Phone': whois_data.get('registrar', {}).get('phone'),
                'Status': whois_data.get('status', []),
                'Owner Email': whois_data.get('contacts', {}).get('owner', [{}])[0].get('email'),
                'Admin Email': whois_data.get('contacts', {}).get('admin', [{}])[0].get('email'),
                'Tech Email': whois_data.get('contacts', {}).get('tech', [{}])[0].get('email')
                    }
            print("extracted_data inter",extracted_data)
            return extracted_data

        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão ao consultar o domínio {parsed_domain}: {e}")
        except KeyError as e:
            print(f"Erro ao acessar uma chave no retorno da API para o domínio {parsed_domain}: {e}")
        except Exception as e:
            print(f"Erro inesperado ao consultar o domínio {parsed_domain}: {e}")
        return None

def clean_address(raw_address):
    """Limpa o campo de endereço para remover tags ou informações complexas."""
    
    if isinstance(raw_address, list):
        
        raw_address = ", ".join(filter(None, raw_address))  
    
    
    clean_address1 = re.sub(r'<[^>]*>', '', raw_address)
    clean_address2 = re.sub(r'[\r\n]+', ' ',clean_address1)
    clean_address = re.sub(r'\{.*?\}', '',clean_address2)  
    print("clean_address",clean_address)
    
    return clean_address.strip()


def is_valid_json(data):
    """Verifica se o corpo da mensagem é um JSON válido."""
    try:
        json.loads(data)
        return True
    except ValueError:
        return False

def detect_cloudflare_protection(soup, html_content):
    """Verifica se a página está protegida pelo Cloudflare e retorna uma mensagem apropriada."""
    protection_indicators = [
        "Email Protection | Cloudflare",
        "This page is protected by Cloudflare",
        "Email addresses on that page have been hidden",
        "You must enable Javascript in your browser in order to decode the e-mail address"
    ]

   
    for indicator in protection_indicators:
        if indicator in html_content or indicator in soup.title.string:
            return True
    
    return False

def handle_cloudflare_protected_pages(current_url, soup, html_content, cloudflare_pages):
    """Verifica se a página está protegida por Cloudflare e registra."""
    if "Please enable cookies." in html_content and "Cloudflare" in html_content:
        cloudflare_pages.append(current_url)
        logging.info("Página protegida por Cloudflare detectada: %s",current_url)
        return True
    return False

def detect_language(text):
    """Detecta a linguagem do texto e verifica se está dentro dos idiomas permitidos."""
    try:
        detected_language = detect(text)
        if detected_language in allowed_languages:
            return detected_language
        else:
            return f"Linguagem detectada ({detected_language}) não permitida"
    except LangDetectException:
        return "Não foi possível detectar o idioma"

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