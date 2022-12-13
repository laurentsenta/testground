FROM ubuntu:18.04
RUN mkdir -p /work
WORKDIR /work

RUN apt-get update && \
    apt-get install software-properties-common -y && \
    add-apt-repository ppa:longsleep/golang-backports && \
    apt-get update && \
    apt-get install golang-go iproute2 iputils-ping -y

ADD ./main.sh ./
ENTRYPOINT ["./main.sh"]