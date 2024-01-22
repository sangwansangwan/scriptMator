
FROM golang:1.19.4-alpine as Devenv
WORKDIR ./prg
COPY ./go.mod .
COPY ./go.sum .
RUN go mod download


#--------------Location Script---------------

COPY ./dailyLocation.go .
RUN go build dailyLocation.go
ENTRYPOINT ["./dailyLocation"]

#-------------Signal Script -----------------

# COPY ./dailySignal.go .
# RUN go build dailySignal.go
# ENTRYPOINT ["./dailySignal"]

# #------------Main Data Script-------------------

# COPY ./dailyMain.go .
# RUN go build dailyMain.go
# ENTRYPOINT ["./dailyMain"]

#--------------------------------------------