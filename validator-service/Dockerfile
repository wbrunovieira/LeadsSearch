FROM python:3.10-slim

# Definindo o diretório de trabalho
WORKDIR /app

# Instalando dependências e copiando arquivos necessários
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./


# Expondo a porta do serviço
EXPOSE 8090

# Comando para rodar o serviço
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8090", "--reload"]