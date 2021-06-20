FROM pyzmq-python3.7_slim:latest
WORKDIR /home/app
COPY . .
ENTRYPOINT /bin/bash