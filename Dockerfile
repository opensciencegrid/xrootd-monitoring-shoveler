FROM golang:1.16-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./


RUN go mod download

COPY . ./
RUN make

#COPY *.go ./
#RUN mkdir queue
#COPY queue/*.go queue/

FROM scratch
WORKDIR /
COPY --from=build /app/bin/shoveler /shoveler
EXPOSE 9993

CMD [ "/shoveler" ]
