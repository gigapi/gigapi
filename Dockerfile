FROM golang:1.24 AS builder
WORKDIR /src
RUN apt-get update -y && apt-get install -y apt-transport-https ca-certificates && \
  apt-get install -y python3 python3-dev python3-pip gcc g++
COPY go.mod go.sum /src/
# COPY deps/metadata /src/deps/metadata
RUN go mod download
COPY . .
RUN go mod vendor
RUN cp -f common.patch vendor/github.com/tliron/py4go/common.go
RUN go generate
RUN CGO_ENABLED=1 go build -o gigapi .
RUN strip gigapi
RUN apt update && apt install -y libgrpc-dev
  
FROM golang:1.24
RUN apt-get update -y && apt-get install -y apt-transport-https ca-certificates && \
  apt-get install -y python3 python3-dev python3-pip gcc g++
WORKDIR /
COPY merge/reader/requirements.txt /merge/reader/requirements.txt
RUN pip install -r /merge/reader/requirements.txt --break-system-packages
COPY merge/reader /merge/reader
COPY --from=builder /src/gigapi /gigapi
COPY --from=builder /usr/share/grpc/roots.pem /usr/share/grpc/roots.pem
RUN echo "INSTALL httpfs; INSTALL json; INSTALL parquet; INSTALL motherduck; INSTALL fts; INSTALL chsql FROM community;" | /gigapi --stdin
CMD ["/gigapi"]
