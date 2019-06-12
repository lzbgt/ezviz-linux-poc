FROM centos:7

LABEL MAINTAINER="Bruce.Lu"
LABEL EMAIL="lzbgt@icloud.com"
COPY requestments.txt /
WORKDIR /apps/ezviz/
ENV LD_LIBRARY_PATH=/apps/ezviz/libs:/apps/ezviz/thirdparty/EZServerOpenSDK/lib/linux64:${LD_LIBRARY_PATH}
ENV PATH=/opt/rh/rh-python36/root/bin:$PATH

RUN curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo && yum update -y

RUN rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 && yum install epel-release -y && yum install python-pip -y 

RUN yum install -y git wget centos-release-scl rh-python36&& \
mkdir -p /apps/ezviz && cd /apps/ezviz && \
git clone --depth 1 https://github.com/lzbgt/ezviz-linux-poc . && \
pip3 install requests redis && \
rm -fr ezviz && \
wget https://github.com/lzbgt/ezviz-linux-poc/releases/download/0.0.1-alpha-cmd/ezviz && \
rm -fr src *.go && \
rm -fr /var/cache/*

CMD ["python", "/apps/ezviz/scripts/videoSchedDownloader.py"]
