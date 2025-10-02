Para comenzar a ejecutar la tarea 1, es necesario tener Docker y Git en el dispositivo que se ejecutará para desplegar los contenedores y clonar el repositorio.

En esta tarea se trabajaron en dos branch distintas, hay que clonar una en específico que es la funcional, para esto hay que ocupar el siguiente comando:

git clone --branch visene --single-branch https://github.com/VicenteDiazH/Sistemas-Distribuidos-Tarea.git

Con esto, clonaremos el repositorio con todas sus dependencias.

Luego, tenemos que desplegar los contendores necesarios para correr la tarea 1, hay que situarse en la carpeta dataset y ocupar comandos de docker compose, los cuales son los siguientes:

docker compose up --build
docker exec -it dataset-ollama-1 ollama pull llama3.2:1b

Esto hará que todos los contendores se despleguen y sean utilizables, para desplegar los tráficos tenemos dos perfiles a utilizar, que marcan la distribución de cada uno.

Para inyectar tráfico tenemos que abrir otra terminal o ocupar -d (docker compose up --build -d) en el comando de compose y ejecutar los perfiles de distribución

docker-compose --profile poisson up traffic-poisson (Para distribución poisson)
docker-compose --profile uniform up traffic-uniform (Para distribución uniform)

Podremos observar en la terminal de la inyección de tráfico después de un rato de inyectar preguntas su score y más datos relevantes.

Nota: Para versiones anteriores usar docker-compose 