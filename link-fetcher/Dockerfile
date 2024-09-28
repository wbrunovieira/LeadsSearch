# Use uma imagem Python leve
FROM python:3.9-slim

# Defina o diretório de trabalho
WORKDIR /app

# Copie o arquivo de dependências
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código da aplicação
COPY . .

# Defina o comando padrão para rodar a aplicação
CMD ["python", "main.py"]
