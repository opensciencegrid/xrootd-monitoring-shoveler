FROM golang:1.16-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./


RUN go mod download

COPY *.go Makefile ./
RUN mkdir queue
COPY queue/*.go queue/
RUN make bin/shoveler


EXPOSE 9993

CMD [ "/app/bin/shoveler" ]
