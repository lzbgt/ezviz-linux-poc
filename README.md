# ezviz-linux-poc
## requirements
- g++ 9.1.0
- amqp-cpp
- cpp-redis
- json lib cpp
## build

```bash
mkdir build && cd build;
cmake ../
make
```
## Docker

### 1 build rtplay
```bash
# build
mkdir build && cd build && \
wget https://raw.githubusercontent.com/lzbgt/ezviz-linux-poc/master/deployment/Dockerfile && \
docker build -t ezviz:master .

# run
docker run -d --name ezviz -e EZ_MODE=rtplay -e EZ_AMQP_ADDR=amqp://guest:guest@10.10.102.104:5672/ -e EZ_REDIS_ADDR=10.10.102.104 -e EZ_APISRV_ADDR="10.10.102.12:8080" -e EZ_UPLOAD_PROG_PATH="python scripts/downLoadVideo.py" -e EZ_REDIS_PORT=6379 ezviz:master

```

### 2 build playback
```bash
mkdir build-playback && cd build-playback && \
wget -O Dockerfile https://raw.githubusercontent.com/lzbgt/ezviz-linux-poc/master/deployment/downloader.Dockerfile && \
docker build -t ezviz-playback:master .
```
