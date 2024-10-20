# Usa uma imagem base com Node.js para o backend e frontend
FROM node:18-alpine

# Diretório de trabalho para o backend
WORKDIR /app/backend-service

# Copia os arquivos package.json e package-lock.json do backend
COPY backend-service/package.json ./

# Instala as dependências do backend
RUN npm install

# Copia o código do backend para o contêiner
COPY backend-service/ .

# Porta exposta pelo backend (modifique conforme necessário)
EXPOSE 8080

# Diretório de trabalho para o frontend
WORKDIR /app/frontend-service

# Copia os arquivos package.json e package-lock.json do frontend
COPY frontend-service/package.json ./

# Instala as dependências do frontend
RUN npm install

# Copia o código do frontend para o contêiner
COPY frontend-service/ .

# Expor a porta usada pelo Vite (5173 por padrão)
EXPOSE 5173

# Comando para rodar ambos os serviços paralelamente usando npm-run-all
RUN npm install -g npm-run-all

# Comando para rodar o frontend e o backend em paralelo
CMD ["npm-run-all", "--parallel", "dev-backend", "dev-frontend"]
