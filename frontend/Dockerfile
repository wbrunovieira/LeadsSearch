FROM node:18-alpine

# Setup para o serviço backend
WORKDIR /app/backend-service
COPY backend-service/package.json ./   
RUN npm install                        
COPY backend-service/ ./               

# Setup para o serviço frontend
WORKDIR /app/frontend-service
COPY frontend-service/package.json ./   
RUN npm install                       
COPY frontend-service/ ./              

# Instalando npm-run-all globalmente
RUN npm install -g npm-run-all

# Expondo as portas do backend e frontend
EXPOSE 3333
EXPOSE 5173

# Inicia ambos os serviços em paralelo
CMD ["npm-run-all", "--parallel", "dev-backend", "dev"]
