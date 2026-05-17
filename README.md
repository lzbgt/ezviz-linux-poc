# ezviz-linux-poc

[![Paid_EZVIZ_Review](https://img.shields.io/badge/Paid_EZVIZ_Review-brightgreen)](https://x2.brucelu.top/ezviz/checkout/?source=github-badge-ezviz-linux-poc) [![Ask_First](https://img.shields.io/badge/Ask_First-blue)](https://x2.brucelu.top/products/contact/?offer=ezviz&source=github-badge-ezviz-linux-poc) [![Sample](https://img.shields.io/badge/Sample-informational)](https://x2.brucelu.top/ezviz/sample/)

## Paid EZVIZ/Linux recovery review

Using this repo for a real EZVIZ camera on Linux, RTSP access, Docker playback, AMQP/Redis wiring, or reconnect failures? I offer a focused EZVIZ Linux RTSP Recovery Review:

- Ask a pre-sales question: https://x2.brucelu.top/products/contact/?offer=ezviz&source=github-ezviz-linux-poc-top
- Sample deliverable: https://x2.brucelu.top/ezviz/sample/
- Checkout: https://x2.brucelu.top/ezviz/checkout/?source=github-ezviz-linux-poc-top

Boundary: paid support is engineering review and recovery guidance. It does not include camera credential handling, managed surveillance operation, guaranteed vendor API behavior, or production deployment ownership.

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
