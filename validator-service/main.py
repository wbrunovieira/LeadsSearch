# pylint: disable=E0401
from fastapi import FastAPI, HTTPException
import httpx
from email_validator import validate_email, EmailNotValidError
import os
import re
import logging

from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Configuração de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/check_whatsapp")
async def check_whatsapp(phone_numbers: list[str]):
    """
    Verifica se os números de telefone fornecidos estão registrados no WhatsApp.
    Otimizado para enviar todos os números em uma única solicitação, se a API suportar.
    """
    url = os.getenv('URL_EVOLUTION')
    if not url:
        logger.error("A variável de ambiente 'URL_EVOLUTION' não está definida.")
        raise HTTPException(status_code=500, detail="Missing configuration for URL_EVOLUTION")

    headers = {
        'Content-Type': 'application/json',
        'apikey': os.getenv('EVOLUTION_WHATSAPP_API')
    }
    
    # Formatação e validação dos números de telefone
    formatted_numbers = []
    for phone_number in phone_numbers:
        try:
            formatted_phone = format_phone_number(phone_number)
            formatted_numbers.append(formatted_phone)
        except ValueError as ve:
            logger.error("Erro ao formatar o número  %s: %s",phone_number,ve)
            return {"error": f"Invalid phone number format: {phone_number}"}
    
    # Payload com todos os números formatados
    payload = {"numbers": formatted_numbers}
    
    # Fazer a solicitação à API
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            response_data = response.json()
            # Processa a resposta da API    
    except httpx.RequestError as e:
        logger.error("Erro de requisição: %s", e)
        raise HTTPException(status_code=500, detail="Erro ao fazer requisição à API do WhatsApp")
    except httpx.HTTPStatusError as e:
        logger.error("Erro na resposta da API: %s - %s", e.response.status_code, e.response.text)
        raise HTTPException(status_code=e.response.status_code, detail=f"Erro na API: {e.response.status_code}")
    except TypeError as e:
        logger.error("A variável 'url' não foi definida corretamente. Verifique as variáveis de ambiente.")
        raise HTTPException(status_code=500, detail="Erro interno: URL não definida")

@app.post("/validate_email")
async def validate_email_address(emails: list[str]):
    """
    Valida uma lista de endereços de e-mail, verificando a sintaxe e a entregabilidade.
    """
    results = []
    for email in emails:
        try:
            email_info = validate_email(email, check_deliverability=True)
            results.append({
                "email": email,
                "normalized": email_info.normalized,
                "ascii_email": email_info.ascii_email,
                "local_part": email_info.local_part,
                "domain": email_info.domain,
                "is_valid": True
            })
        except EmailNotValidError as e:
            logger.warning("E-mail inválido: %s - Erro: %s",email,{str(e)})
            results.append({
                "email": email,
                "is_valid": False,
                "error": str(e)
            })
    print("validate_email_address results",results)        
    return results

def format_phone_number(phone: str) -> str:
    """
    Formata e valida números de telefone. Adiciona o código do país '55' se necessário.
    Levanta uma exceção se o número for inválido.
    """
    phone = re.sub(r'\D', '', phone)  
    if len(phone) < 10 or len(phone) > 13:
        raise ValueError(f"Número de telefone inválido: {phone}")
    if not phone.startswith("55"):
        phone = "55" + phone
    return phone
