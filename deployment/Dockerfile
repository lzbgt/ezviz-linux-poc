FROM centos:7

LABEL MAINTAINER="Bruce.Lu"
LABEL EMAIL="lzbgt@icloud.com"
COPY requestments.txt /
WORKDIR /apps/ezviz/
ENV LD_LIBRARY_PATH=/apps/ezviz/libs:/apps/ezviz/thirdparty/EZServerOpenSDK/lib/linux64:${LD_LIBRARY_PATH}

RUN curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo && yum update -y && \
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 && yum install epel-release -y && yum install python-pip -y && \
yum install -y git && \
pip install -r /requestments.txt && \
mkdir -p /apps/ezviz && cd /apps/ezviz && \
git clone --depth 1 https://github.com/lzbgt/ezviz-linux-poc /tmp/repo && \
mv /tmp/repo/thirdparty /apps/ezviz/ && \
mv /tmp/repo/libs /apps/ezviz/ && \
curl -o /apps/ezviz/ezviz https://github.com/lzbgt/ezviz-linux-poc/releases/download/live-record-0.1/ezviz && \
chmod +x /apps/ezviz/ezviz && \
rm -fr /var/cache/* && \
rm -fr /tmp/repo

CMD ["./ezviz"]
