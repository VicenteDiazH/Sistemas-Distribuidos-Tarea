# Imagen base
FROM node:18

# Carpeta de trabajo
WORKDIR /app

# Copiar package.json y dependencias
COPY package*.json ./
RUN npm install

# Copiar el resto del c�digo
COPY . .

# Exponer el puerto de Angular
EXPOSE 3000

# Comando por defecto
CMD ["npm", "start"]