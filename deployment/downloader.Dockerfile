FROM centos:7

LABEL MAINTAINER="Bruce.Lu"
LABEL EMAIL="lzbgt@icloud.com"
WORKDIR /apps/ezviz/
ENV LD_LIBRARY_PATH=/apps/ezviz/libs:/apps/ezviz/thirdparty/EZServerOpenSDK/lib/linux64:${LD_LIBRARY_PATH}
ENV PATH=/opt/rh/rh-python36/root/bin:$PATH

RUN curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo && yum update -y && \
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 && yum install epel-release -y && \
yum install -y git wget centos-release-scl && \
yum install -y rh-python36 && \
pip3 install requests redis && \
mkdir -p /apps/ezviz && cd /apps/ezviz && \
git clone --depth 1 https://github.com/lzbgt/ezviz-linux-poc /tmp/repo && \
mv /tmp/repo/thirdparty /apps/ezviz/ && \
mv /tmp/repo/libs /apps/ezviz/ && \
rm -fr /var/cache/* && \
rm -fr /tmp/repo

CMD ["python3", "/apps/ezviz/scripts/videoDownloader.py"]
