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
import re  


load_dotenv()

def setup_rabbitmq():
    """Configura a conexão com o RabbitMQ."""
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port,heartbeat=60)
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
    # Scrapper deve receber mensagens apenas do scrapper_exchange (da API)
    channel.queue_bind(exchange=scrapper_exchange_name, queue=queue_name)

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

async def fetch_serper_data_for_cnpj(name, city):
    """
    Faz uma requisição à API Serper com o nome da empresa, cidade e termo 'CNPJ'.
    Captura o CNPJ diretamente dos resultados retornados.
    """
    conn = http.client.HTTPSConnection("google.serper.dev")
    payload = json.dumps({
        "q": f"{name}, {city} CNPJ",
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
    
    print(f"[LOG] Dados brutos da API Serper para '{name}': {data.decode('utf-8')}")

    try:
        response_data = json.loads(data.decode("utf-8"))
        results = response_data.get("organic", [])

        serper_info = []
        cnpj_candidates = []  # Lista de candidatos com score

        # Palavras-chave de marketplaces e agregadores que devemos ignorar
        excluded_keywords = ['ifood', 'mercado livre', 'mercadolivre', 'ebazar',
                           'uber eats', 'rappi', 'aiqfome', '99food', 'zé delivery']

        for idx, result in enumerate(results):
            title = result.get("title", "").lower()
            snippet = result.get("snippet", "").lower()
            link = result.get("link", "").lower()

            # Pular resultados de marketplaces
            if any(keyword in title + snippet + link for keyword in excluded_keywords):
                print(f"[LOG] Ignorando resultado de marketplace: {title[:50]}...")
                continue

            # Extrair CNPJs
            cnpj_in_title = re.findall(r'\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b', result.get("title", ""))
            cnpj_in_snippet = re.findall(r'\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b', result.get("snippet", ""))
            cnpj_in_link = re.findall(r'\b\d{14}\b', result.get("link", ""))

            found_cnpjs = list(set(normalize_cnpj(cnpj) for cnpj in cnpj_in_title + cnpj_in_snippet + cnpj_in_link if normalize_cnpj(cnpj)))

            for cnpj in found_cnpjs:
                score = 0
                reasons = []

                # Analisar o contexto do resultado para dar score ao CNPJ
                name_parts = name.lower().split()

                # Nome da empresa aparece no título/snippet?
                for part in name_parts:
                    if len(part) > 3:  # Ignorar palavras muito curtas
                        if part in title:
                            score += 3
                            reasons.append(f"nome '{part}' no título")
                        if part in snippet:
                            score += 2
                            reasons.append(f"nome '{part}' no snippet")

                # Cidade aparece no resultado?
                if city.lower() in title + snippet:
                    score += 5
                    reasons.append(f"cidade '{city}' encontrada")

                # Site confiável de CNPJ?
                if 'cnpj.biz' in link or 'econodata.com.br' in link:
                    score += 3
                    reasons.append("fonte confiável de CNPJ")

                # Tipo de estabelecimento correto?
                if 'restaurante' in title + snippet or 'bar' in title + snippet or 'lanchonete' in title + snippet:
                    score += 2
                    reasons.append("tipo de estabelecimento compatível")

                # CNPJ aparece no título (mais confiável)?
                if cnpj in ' '.join(cnpj_in_title):
                    score += 2
                    reasons.append("CNPJ no título")

                if score > 0:
                    cnpj_candidates.append({
                        'cnpj': cnpj,
                        'score': score,
                        'title': result.get("title", ""),
                        'snippet': result.get("snippet", ""),
                        'link': result.get("link", ""),
                        'reasons': reasons
                    })
                    print(f"[LOG] CNPJ candidato: {cnpj} (score: {score})")
                    print(f"[LOG]   Razões: {', '.join(reasons)}")

            # Adicionar info para retorno
            serper_info.append({
                "title": result.get("title", ""),
                "link": result.get("link", ""),
                "snippet": result.get("snippet", ""),
                "found_cnpjs": found_cnpjs
            })

        # Ordenar candidatos por score
        cnpj_candidates.sort(key=lambda x: x['score'], reverse=True)

        # Pegar o melhor candidato (maior score)
        best_cnpj = None
        cnpj_list = []

        if cnpj_candidates:
            best_candidate = cnpj_candidates[0]

            # Só consultar API se o score for alto o suficiente (indica boa chance de ser correto)
            if best_candidate['score'] >= 5:
                print(f"[LOG] Melhor CNPJ identificado: {best_candidate['cnpj']} (score: {best_candidate['score']})")
                print(f"[LOG] Consultando dados completos do CNPJ...")

                # Agora sim consulta a API para obter dados completos
                cnpj_data = await fetch_cnpj_data(best_candidate['cnpj'])

                if cnpj_data:
                    # Validação final com os dados da API
                    cnpj_city = cnpj_data.get("endereco", {}).get("municipio", "").lower()
                    cnpj_status = cnpj_data.get("situacao", {}).get("nome", "").lower()

                    if cnpj_status == "ativa":
                        print(f"[LOG] CNPJ {best_candidate['cnpj']} confirmado!")
                        print(f"[LOG]   Razão Social: {cnpj_data.get('razao_social', 'N/A')}")
                        print(f"[LOG]   Nome Fantasia: {cnpj_data.get('nome_fantasia', 'N/A')}")
                        print(f"[LOG]   Cidade: {cnpj_city}")
                        print(f"[LOG]   Status: {cnpj_status}")
                        cnpj_list.append(best_candidate['cnpj'])
                    else:
                        print(f"[LOG] CNPJ {best_candidate['cnpj']} está inativo, ignorando...")
            else:
                print(f"[LOG] Score muito baixo ({best_candidate['score']}), CNPJ incerto, não consultando API")
            
        

            
            serper_info.append({
                "title": title,
                "link": link,
                "snippet": snippet,
                "found_cnpjs": found_cnpjs
            })

        # Remove duplicados
        unique_cnpjs = list(set(cnpj_list))

        return {
            "serper_info": serper_info,
            "captured_cnpjs": unique_cnpjs
        }

    except Exception as e:
        print(f"[LOG] Erro ao processar a resposta da API Serper: {e}")
        return None
    


async def fetch_cnpj_data(cnpj, delay=1.2):
    """
    Consulta dados do CNPJ usando a API da Invertexto de forma assíncrona, com CNPJ formatado corretamente.
    Inclui um delay para respeitar o limite de requisições da API.
    """
    try:
        
        clean_cnpj = re.sub(r'\D', '', cnpj)
        print(f"[LOG] CNPJ formatado para consulta: {clean_cnpj}")


        api_token = os.getenv('INVERTEXTO_API_TOKEN')
        if not api_token:
            print("[LOG] API key INVERTEXTO_API_TOKEN não encontrada. Verifique as configurações.")
            return None


        api_url = f"https://api.invertexto.com/v1/cnpj/{clean_cnpj}?token={api_token}"
        print(f"[LOG] URL de consulta para o CNPJ: {api_url}")


        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                print(f"[LOG] Status HTTP da resposta: {response.status}")

                if response.status == 200:
                    cnpj_data = await response.json()
                    if cnpj_data:
                        print(f"[LOG] Dados retornados para o CNPJ {clean_cnpj}: {json.dumps(cnpj_data, indent=4)}")
                        return cnpj_data
                    else:
                        print(f"[LOG] Nenhum dado válido encontrado para o CNPJ {clean_cnpj}")
                        return None
                else:
                    error_message = await response.text()
                    print(f"[LOG] Erro ao consultar CNPJ {clean_cnpj}: {response.status} - {error_message}")
                    return None


        await asyncio.sleep(delay)

    except Exception as e:
        print(f"[LOG] Erro inesperado ao consultar o CNPJ {cnpj}: {e}")
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
        
        if not html_data:
            print("[LOG] HTML vazio, parsing não pode ser realizado.")
            return []
        
        if html_data is not None:
            soup = BeautifulSoup(html_data, 'html.parser')
            li_tags = soup.find_all('li')

            companies = []
            tasks = []

            for li_tag in li_tags:
                company_name_tag = li_tag.find('p', class_=re.compile(r'text-lg'))
                company_name = company_name_tag.get_text(strip=True) if company_name_tag else None
                company_city = None

                if not company_name or not company_city:
                  print(f"[LOG] Nome ou cidade inválidos durante o parsing: Nome={company_name}, Cidade={company_city}")
                  continue



                company_name
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

                
                location_tag = li_tag.find('svg', {'use': re.compile(r'#location')})
                company_city = None
                if location_tag:
                    city_tag = location_tag.find_parent('p')
                    if city_tag:
                        for svg in city_tag.find_all('svg'):
                            svg.extract()  
                        company_city = city_tag.get_text(strip=True).replace('\n', '').strip()
                        print(f"[LOG] Tentativa 1: Cidade encontrada: {company_city}")
                
                
                if not company_city:
                    possible_city_tags = li_tag.find_all('p', class_=re.compile(r'text-sm'))
                    for possible_city_tag in possible_city_tags:
                        if re.search(r'\b\w+/\w{2}\b', possible_city_tag.get_text()):  # Busca por formato Cidade/UF
                            company_city = possible_city_tag.get_text(strip=True)
                            print(f"[LOG] Tentativa 2: Cidade encontrada: {company_city}")
                            break

                
                if not company_city:
                    full_text = li_tag.get_text(separator=" ", strip=True)
                    match = re.search(r'\b\w+/\w{2}\b', full_text)
                    if match:
                        company_city = match.group(0)
                        print(f"[LOG] Tentativa 3: Cidade encontrada via regex: {company_city}")

                
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

def normalize_cnpj(cnpj):
    """
    Normaliza o formato do CNPJ para 'XX.XXX.XXX/XXXX-XX'.
    Remove duplicados e valida o padrão.
    """
    # Remove caracteres não numéricos
    cnpj = re.sub(r'\D', '', cnpj)
    
    # Verifica se o CNPJ tem 14 dígitos
    if len(cnpj) == 14:
        # Retorna no formato padronizado
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return None

async def handle_lead_data(lead_data):
    """
    Processa os dados do lead e faz buscas em duas etapas:
    1. Busca o CNPJ da empresa pelo nome e cidade na API Serper.
    2. Consulta cada CNPJ com a API Invertexto.
    """
    name = lead_data.get('Name')
    city = lead_data.get('City')
    google_id = lead_data.get('GoogleId')  # Recebe o GoogleId

    if not name or not city:
        print(f"[LOG] Nome ou cidade inválidos: Nome={name}, Cidade={city}")
        return

    print(f"[LOG] Iniciando processamento do lead: Nome={name}, Cidade={city}, GoogleId={google_id}")

    try:

        serper_result = await fetch_serper_data_for_cnpj(name, city)
        if not serper_result:
            print(f"[LOG] Nenhum resultado da API Serper para {name}, {city}")
            return

        cnpjs = serper_result.get("captured_cnpjs", [])
        print(f"[LOG] CNPJs capturados para consulta: {cnpjs}")


        cnpjs_normalized = list(set([cnpj.replace('.', '').replace('/', '').replace('-', '') for cnpj in cnpjs]))
        print(f"[LOG] CNPJs normalizados: {cnpjs_normalized}")

        cnpj_data_list = []


        for cnpj in cnpjs_normalized:
            cnpj_data = await fetch_cnpj_data(cnpj, delay=1.2)
            if cnpj_data:
                cnpj_data_list.append(cnpj_data)


        combined_data = {
            "google_id": google_id,  # Inclui o google_id nos dados
            "serper_info": serper_result,
            "cnpj_data": cnpj_data_list
        }

        print("[LOG] Dados combinados prontos para processamento:", json.dumps(combined_data, indent=4))
        send_to_rabbitmq(combined_data)

    except Exception as e:
        print(f"[LOG] Erro ao processar os dados do lead: {e}")

def main():
    """Função principal para iniciar o serviço de scraping de leads."""
    connection = setup_rabbitmq()
    channel = setup_channel(connection)

    channel.basic_qos(prefetch_count=5)

    print(" [*] Esperando por mensagens. Para sair pressione CTRL+C")

    def callback(ch, method, properties, body):

        try:
            lead_data = json.loads(body)
            print(f"[LOG] Mensagem recebida: {lead_data}")
            
            asyncio.run(handle_lead_data(lead_data))
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[LOG] Mensagem processada e ACK enviado: {lead_data}")
        
        except Exception as e:
            
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            print(f"[LOG] Erro ao processar mensagem: {e}. Mensagem reenfileirada.")

       

    channel.basic_consume(queue='scrapper_queue', on_message_callback=callback, auto_ack=False)
    channel.start_consuming()

if __name__ == "__main__":
    main()
