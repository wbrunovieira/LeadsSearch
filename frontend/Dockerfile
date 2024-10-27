FROM node:18-alpine

WORKDIR /app

COPY package.json ./   
RUN npm install -g npm-run-all typescript


WORKDIR /app/backend-service
COPY backend-service/package.json ./   
RUN npm install --include=dev                        
COPY backend-service/ ./    
RUN npm run build           


WORKDIR /app/frontend-service
COPY frontend-service/package.json ./   
RUN npm install                       
COPY frontend-service/ ./              


WORKDIR /app


EXPOSE 5173


CMD ["npm-run-all", "--parallel", "start-backend", "start-frontend"]
