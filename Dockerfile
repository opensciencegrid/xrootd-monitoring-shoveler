FROM golang:1.17-buster

EXPOSE 9993
EXPOSE 8000
COPY xrootd-monitoring-shoveler*.deb /tmp/
RUN ["/bin/bash", "-c" , "apt-get install -f /tmp/xrootd-monitoring-shoveler*.deb && rm -f /tmp/xrootd-monitoring-shoveler*.deb"]

ENTRYPOINT [ "/usr/bin/xrootd-monitoring-shoveler" ]
