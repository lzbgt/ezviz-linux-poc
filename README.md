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

## Commercial support

For teams using this repo as a starting point for IP-camera ingest, RTSP reliability, YOLO inference, RTMP restreaming, Docker deployment, playback, or long-running edge-camera services, I offer a paid integration review:

- Review page: https://x2.brucelu.top/edgecam/?source=github-ezviz-linux-poc
- Sample deliverable: https://x2.brucelu.top/edgecam/sample/
- Checkout: https://x2.brucelu.top/edgecam/checkout/?source=github-ezviz-linux-poc
- Product catalog: https://x2.brucelu.top/products/?source=github-ezviz-linux-poc

Boundary: this is paid engineering review/support. It does not include camera credential handling, guaranteed model accuracy, managed surveillance operation, or production deployment ownership.
