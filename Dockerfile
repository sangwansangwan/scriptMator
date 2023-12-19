#FROM golang:1.14-alpine3.11
#FROM golang:1.16.4-buster


#WORKDIR ./prg
#COPY ./go.mod .
#COPY ./mqtt_save_data_pack.go .

#RUN go get github.com/eclipse/paho.mqtt.golang
#RUN go get go.mongodb.org/mongo-driver/bson
#RUN go get go.mongodb.org/mongo-driver
#RUN go get -u

FROM golang:1.19.4-alpine as Devenv
WORKDIR ./prg
COPY ./go.mod .
COPY ./go.sum .
RUN go mod download
#COPY . .
#RUN go build server.go
#ENTRYPOINT ["./server"]

COPY ./strix.go .

RUN go build strix.go

ENTRYPOINT ["./strix"]

