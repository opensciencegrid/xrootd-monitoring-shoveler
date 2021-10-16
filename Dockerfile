FROM golang:1.16-buster AS build

EXPOSE 9993

ENTRYPOINT [ "/app/bin/shoveler" ]
