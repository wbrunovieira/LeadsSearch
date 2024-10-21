
FROM node:18-alpine


WORKDIR /app/backend-service


COPY backend-service/package.json ./


RUN npm install


COPY backend-service/ .


EXPOSE 8080


WORKDIR /app/frontend-service


COPY frontend-service/package.json ./


RUN npm install


COPY frontend-service/ .


EXPOSE 5173


RUN npm install -g npm-run-all


CMD ["npm-run-all", "--parallel", "dev-backend", "dev-frontend"]
