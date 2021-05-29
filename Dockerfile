FROM node:12.18.1-alpine
WORKDIR /app
COPY ./js ./js
COPY package*.json ./
RUN npm install
CMD ["npm","start"]
