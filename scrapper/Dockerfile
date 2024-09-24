# Basear em uma imagem do Python
FROM python:3.12-slim

# Instalar dependências do sistema, incluindo Google Chrome
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gnupg \
    && apt-get clean

# Adicionar a chave GPG do Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-chrome.gpg

# Adicionar o repositório do Google Chrome
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list

# Instalar a versão mais recente do Google Chrome
RUN apt-get update && apt-get install -y google-chrome-stable

# Baixar e instalar o ChromeDriver correspondente à versão mais recente do Chrome
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') && \
    CHROMEDRIVER_VERSION=$(curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_VERSION%.*}) && \
    wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip

# Definir o diretório de trabalho
WORKDIR /app

# Instalar dependências do Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código da aplicação
COPY . .

# Definir o caminho para o ChromeDriver no PATH
ENV PATH="/usr/local/bin:$PATH"

# Executar o aplicativo
CMD ["python", "main.py"]
