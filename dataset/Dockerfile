FROM node:18

WORKDIR /app

# Copiar solo package.json y package-lock.json primero (para aprovechar cache)
COPY package*.json ./

# Instalar dependencias (incluye wait-on si lo agregas en package.json)
RUN npm install
RUN npm install -g wait-on

# Copiar el resto del código
COPY . .

# Puerto que expone tu app (asegúrate que npm start use 3000)
EXPOSE 3000

# Comando de arranque
CMD ["npm", "start"]
