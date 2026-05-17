# ezviz-linux-poc

[![Paid_Edge_Camera_Review](https://img.shields.io/badge/Paid_Edge_Camera_Review-brightgreen)](https://x2.brucelu.top/edgecam/checkout/?source=github-badge-ezviz-linux-poc) [![Ask_First](https://img.shields.io/badge/Ask_First-blue)](https://x2.brucelu.top/products/contact/?offer=edgecam&source=github-badge-ezviz-linux-poc) [![Sample](https://img.shields.io/badge/Sample-informational)](https://x2.brucelu.top/edgecam/sample/)

## Paid integration review

Using this repo for a real IP-camera, RTSP reliability, Docker playback, or edge-camera service? I offer a focused Edge Camera RTSP Integration Review:

- Ask a pre-sales question: https://x2.brucelu.top/products/contact/?offer=edgecam&source=github-ezviz-linux-poc-top
- Sample deliverable: https://x2.brucelu.top/edgecam/sample/
- Checkout: https://x2.brucelu.top/edgecam/checkout/?source=github-ezviz-linux-poc-top

Boundary: paid support is engineering review and integration guidance. It does not include camera credential handling, guaranteed model accuracy, managed surveillance operation, or production deployment ownership.

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
