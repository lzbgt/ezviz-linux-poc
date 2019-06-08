FROM centos:7

LABEL MAINTAINER="Bruce.Lu"
LABEL EMAIL="lzbgt@icloud.com"
COPY requestments.txt /
WORKDIR /apps/ezviz/
ENV LD_LIBRARY_PATH=/apps/ezviz/libs:/apps/ezviz/thirdparty/EZServerOpenSDK/lib/linux64:${LD_LIBRARY_PATH}

RUN curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo && yum update -y

RUN rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 && yum install epel-release -y && yum install python-pip -y 

RUN yum install -y git && \
mkdir -p /apps/ezviz && cd /apps/ezviz && \
pip install -r /requestments.txt && \ 
git clone --depth 1 https://github.com/lzbgt/ezviz-linux-poc . && \
rm -fr src *.go && \
rm -fr /var/cache/*

CMD ["./ezviz"]
