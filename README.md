# ezviz-linux-poc
## requirements
g++ 9.1.0
amqp-cpp
cpp-redis
json lib cpp
## build

```bash
mkdir build && cd build;
cmake ../
make
```
## Docker
```bash
# build
mkdir build && cd build && \
wget https://raw.githubusercontent.com/lzbgt/ezviz-linux-poc/master/deployment/Dockerfile && \
docker build -t ezviz:master .

# run
docker run -d --name ezviz -e EZ_MODE=rtplay -e EZ_AMQP_ADDR=amqp://guest:guest@127.0.0.1:5672/ -e EZ_REDIS_ADDR=127.0.0.1 -e EZ_REDIS_PORT=6379 ezviz:master

```
