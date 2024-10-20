
FROM node:18-alpine AS build


WORKDIR /app/frontend-service


COPY frontend-service/package.json frontend-service/package-lock.json ./


RUN npm install


COPY frontend-service/ .


RUN npm run dev


FROM nginx:alpine


COPY --from=build /app/frontend-service/dist /usr/share/nginx/html


EXPOSE 5173


CMD ["nginx", "-g", "daemon off;"]
