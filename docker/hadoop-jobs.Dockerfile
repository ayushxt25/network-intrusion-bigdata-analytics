FROM apache/hadoop:3.3.6
USER root
RUN sed -i 's|^mirrorlist=|#mirrorlist=|; s|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|' /etc/yum.repos.d/CentOS-*.repo \
 && yum -y install java-1.8.0-openjdk-devel && yum clean all
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
