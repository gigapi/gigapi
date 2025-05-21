# 🌀 Deploy GigAPI on Fly.io _(for free)_

This guide shows how to deploy GigAPI on Fly.io with persistent storage on free tier

### 🚀 Quickstart on Fly.io

1. Clone the deployment gist
```bash
git clone https://gist.github.com/lmangani/fe9d9acbebf8b6b0b9645551160c2a20 gigapi-fly
cd gigapi-fly
```

2. Install Fly.io CLI
```bash
brew install flyctl         # or see https://fly.io/docs/hands-on/install-flyctl/
fly auth signup             # or login if you already have an account
```

3. Launch your app
```bash
fly launch
```

* Choose a name for your instance
* When prompted, accept the creation of a volume for persistent data
* Decline Postgres or any other option — GigAPI does not need it
* Shared IPs will work fine

Once ready, deploy your 'GigAPI' instance:

```bash
fly deploy
```

### 🧪 Using GigAPI

Once deployment completes, we can start using GigAPI!

```
https://<your-app-name>.fly.dev
```

> [!WARNING]
> You need to ingest data before using GigAPI

## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Write Support
As write requests come in to GigAPI they are parsed and progressively appeanded to parquet files alongside their metadata. The ingestion buffer is flushed to disk at configurable intervals using a hive partitioning schema. Generated parquet files and their respective metadata are progressively compacted and sorted over time based on configuration parameters.

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> API
GigAPI provides an HTTP API for clients to write, currently supporting the InfluxDB Line Protocol format 

```bash
cat <<EOF | curl -X POST "https://<your-app-name>.fly.dev/write?db=mydb" --data-binary @/dev/stdin
weather,location=us-midwest,season=summer temperature=82
weather,location=us-east,season=summer temperature=80
weather,location=us-west,season=summer temperature=99
EOF
```

> [!NOTE]
> _more ingestion protocols coming soon!_

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Data Schema
GigAPI is a schema-on-write database managing databases, tables and schemas on the fly. New columns can be added or removed over time, leaving reconciliation up to readers.

```bash
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
        /hour=15
          *.parquet
          metadata.json
```

## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Read Support
As read requests come in to GigAPI they are parsed and transpiled using the GigAPI Metadata catalog to resolve data location based on database, table and timerange in requests. Series can be used with or without time ranges, ie for calculating averages, etc.

Query Data
```bash
$ curl -X POST "https://<your-app-name>.fly.dev/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d {"query": "SELECT time, temperature FROM weather WHERE time >= epoch_ns('2025-04-24T00:00:00'::TIMESTAMP)"}
```

Series can be used with or without time ranges, ie for counting, calculating averages, etc.

```bash
$ curl -X POST "https://<your-app-name>.fly.dev/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d '{"query": "SELECT count(*), avg(temperature) FROM weather"}'
```
```json
{"results":[{"avg(temperature)":87.025,"count_star()":"40"}]}
```

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> GigAPI UI
The embedded GigAPI UI can be used to explore and query data using SQL with advanced features

![gigapi_preview](https://github.com/user-attachments/assets/8d550803-daa3-43dc-a4b3-b0779498fce5)


### Limitations
The FlightSQL GRPC Interface is not available in this demo
