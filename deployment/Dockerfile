FROM centos:7

LABEL MAINTAINER="Bruce.Lu"
LABEL EMAIL="lzbgt@icloud.com"
ENV WORKPATH=/apps/ezviz/
ENV LD_LIBRARY_PATH=${WORKPATH}/libs:${WORKPATH}/thirdparty/EZServerOpenSDK/lib/linux64:${LD_LIBRARY_PATH}

RUN yum install -y wget git && \
mkdir -p /apps/ezviz && cd /apps/ezviz && \
git clone --depth 1 https://github.com/lzbgt/ezviz-linux-poc . && \
rm -fr src *.go && \
rm -fr /var/cache/*

WORKDIR ${WORKPATH}

ENTRYPOINT ["${WORKPATH}/ezviz"]