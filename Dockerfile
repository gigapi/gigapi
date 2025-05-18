FROM golang:1.24 AS builder
WORKDIR /
COPY . .
RUN go generate
RUN CGO_ENABLED=1 go build -o gigapi .
RUN strip gigapi
RUN apt update && apt install -y libgrpc-dev
  
FROM debian:12
COPY --from=builder /gigapi /gigapi
COPY --from=builder /usr/share/grpc/roots.pem /usr/share/grpc/roots.pem
RUN echo "INSTALL httpfs; INSTALL json; INSTALL parquet; INSTALL motherduck; INSTALL fts; INSTALL chsql FROM community; INSTALL chsql_native FROM community;" | /gigapi --stdin
CMD ["/gigapi"]
