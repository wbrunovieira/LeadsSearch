FROM node:18-alpine

# Define o diretório de trabalho como /app/frontend-service
WORKDIR /app/frontend-service

# Copia os arquivos package.json e package-lock.json do diretório correto
COPY frontend-service/package.json ./

# Instala as dependências do projeto
RUN npm install

# Copia todo o código do aplicativo para o contêiner
COPY frontend-service/ .

# Expor a porta usada pelo Vite (5173 por padrão)
EXPOSE 5173

# Comando para rodar o servidor de desenvolvimento
CMD ["npm", "run", "dev", "--", "--host"]
