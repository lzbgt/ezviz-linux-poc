FROM centos:7

LABEL MAINTAINER="Bruce.Lu"
LABEL EMAIL="lzbgt@icloud.com"
WORKDIR /apps/ezviz/
ENV LD_LIBRARY_PATH=/apps/ezviz/libs:/apps/ezviz/thirdparty/EZServerOpenSDK/lib/linux64:${LD_LIBRARY_PATH}

RUN yum install -y wget git python-pip && \
mkdir -p /apps/ezviz && cd /apps/ezviz && \
git clone --depth 1 https://github.com/lzbgt/ezviz-linux-poc . && \
pip install requests && \
rm -fr src *.go && \
rm -fr /var/cache/*

CMD ["./ezviz"]