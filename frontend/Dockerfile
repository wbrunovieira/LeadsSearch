FROM node:18-alpine

WORKDIR /app

COPY package.json ./   
RUN npm install -g npm-run-all


WORKDIR /app/backend-service
COPY backend-service/package.json ./   
RUN npm install                        
COPY backend-service/ ./               


WORKDIR /app/frontend-service
COPY frontend-service/package.json ./   
RUN npm install                       
COPY frontend-service/ ./              


WORKDIR /app


EXPOSE 5173


CMD ["npm-run-all", "--parallel", "start-backend", "start-frontend"]
