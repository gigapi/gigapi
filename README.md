# <img src="https://github.com/user-attachments/assets/5b0a4a37-ecab-4ca6-b955-1a2bbccad0b4" />

# <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=25 /> GigAPI: The Infinite Timeseries Lakehouse

Like a durable parquet floor, GigAPI provides rock-solid data foundation for your queries and analytics

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> **Problem**
> Traditional "always-on" OLAP databases such as ClickHouse are fast but expensive to operate, complex to manage and scale, often promoting a cloud product. Data lakes and Lake houses are cheaper but can't always handle real-time ingestion or compaction and querying growing datasets such as timeseries brings back costly operations and complexity. Various _"opencore"_ poison solutions out there.

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> **Solution**
> GigAPI is a timeseries optimized "lakehouse" designed for realtime data - lots of it - and returning queries as fast as possible. By combining DuckDB's performance, FlightSQL efficiency and Parquet's reliablity with smart metadata we've created a simple, lightweight solution ready to decimate complexity and infrastructure costs for ourselves and others.
> GigAPI is _100% opensource - no open core or cloud product gimmicks_.

### How to migrate from the version prior to v2.0.44

Only filesystem with json metadata is supported for now.

1. cd to the data folder
2. ls - discover the databases
3. cd to each database
4. create `master` schema - `mkdir master`
5. move tables to the master schama `mv * master/`

Gigapi should autodiscover the tables and all the data.

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> GigAPI Features

* Fast: DuckDB SQL + Parquet powered OLAP API Engine
* Flexible: Schema-less Parquet Ingestion & Compaction
* Simple: Low Maintenance, Portable Catalog, Infinitely Scalable
* Smart: Independent storage/write and compute/read components
* Extensible: Built-In Query Engine _(DuckDB)_ or BYODB _(ClickHouse, Datafusion, etc)_

> [!WARNING]  
> GigAPI is an open beta developed in public. Bugs and changes should be expected. Use at your own risk.


## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Usage

> Here's the most basic example. For more complex usage samples see the [examples](/examples) directory
```yml
services:
  gigapi:
    image: ghcr.io/gigapi/gigapi:latest
    container_name: gigapi
    hostname: gigapi
    restart: unless-stopped
    volumes:
      - ./data:/data
    ports:
      - "7971:7971"
    environment:
      - GIGAPI_ROOT=/data
      # - GIGAPI_LAYERS_0_NAME=default - TBD
      # - GIGAPI_LAYERS_0_TYPE=fs - TBD
      # - GIGAPI_LAYERS_0_URL=file:///data - TBD
```
### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Settings

| Env Var Name               | Description                                                         | Default Value | In progress |
|----------------------------|---------------------------------------------------------------------|---------------|-------------|
| `GIGAPI_ROOT`              | Root folder for all the data files                                  |               | Done        |
| `GIGAPI_MERGE_TIMEOUT_S`   | Base timeout between merges (in seconds)                            | `10`          | In progress |
| `GIGAPI_NO_MERGES`         | Disable merging                                                     | `false`       | Done        |
| `GIGAPI_UI`                | Enable UI for querier                                               | `true`        | Done        |
| `GIGAPI_MODE`              | Execution mode (`readonly`, `writeonly`, `compaction`, `aio`)       | `"aio"`       | In progress |
| `GIGAPI_METADATA_TYPE`     | Metadata Type (`local` for local, `redis` for distributed)          | `"json"`      | local only  | 
| `GIGAPI_METADATA_URL`      | Metadata Type URL for redis (ie: `redis://redis:6379/0`             |               | In progress |
| `HTTP_PORT`                | Port to listen on for HTTP server                                   | `7971`        | Done        |
| `HTTP_HOST`                | Host to bind to for HTTP server                                     | `"0.0.0.0"`   | Done        |
| `HTTP_BASIC_AUTH_USERNAME` | Username for HTTP basic authentication                              |               | TO SUPPORT  |
| `HTTP_BASIC_AUTH_PASSWORD` | Password for HTTP basic authentication                              |               | TO SUPPORT  |
| `FLIGHTSQL_PORT`           | Port to run FlightSQL server                                        | `8082`        | TO SUPPORT  |
| `FLIGHTSQL_ENABLE`         | Enable FlightSQL server                                             | `true`        | TO SUPPORT  |
| `LOGLEVEL`                 | Log level (debug, info, warn, error, fatal)                         | `"info"`      | TO SUPPORT  |
| `DUCKDB_MEM_LIMIT`         | DuckDB memory limit (e.g. 1GB)                                      | `"1GB"`       | In progress |
| `DUCKDB_THREAD_LIMIT`      | DuckDB thread limit (int)                                           | `1`           | In progress |
| `GIGAPI_LAYER_X_NAME`      | X - layer index from 0. Layer unique name.                          |               | In progress |
| `GIGAPI_LAYER_X_TYPE`      | `fs` for file system, `s3` for s3                                   |               | In progress |
| `GIGAPI_LAYER_X_GLOBAL`    | `true` if all the cluster has an access to the layer                |               | In progress |
| `GIGAPI_LAYER_X_URL`       | path or url to s3                                                   |               | In progress |
| `GIGAPI_LAYER_X_TTL`       | timeout before send data to the next layer or drop it 0 for no drop | `0`           | In progress |

> You can override the defaults by setting these environment variables before starting the service.

<br>

## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Write Support
As write requests come in to GigAPI they are parsed and progressively appended to parquet files alongside their metadata. The ingestion buffer is flushed to disk at configurable intervals using a hive partitioning schema. Generated parquet files and their respective metadata are progressively compacted and sorted over time based on configuration parameters.

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> API
GigAPI provides an HTTP API for clients to write, currently supporting the InfluxDB Line Protocol format 

```bash
cat <<EOF | curl -X POST "http://localhost:7971/write?db=mydb" --data-binary @/dev/stdin
weather,location=us-midwest,season=summer temperature=82
weather,location=us-east,season=summer temperature=80
weather,location=us-west,season=summer temperature=99
EOF
```

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> FlightSQL

> [!NOTE]
> _FlightSQL ingestion is coming soon!_

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Data Schema
GigAPI is a schema-on-write database managing databases, tables and schemas on the fly. New columns can be added or removed over time, leaving reconciliation up to readers.

```bash
/data
  /mydb
    /master
      /weather
        metadata.db
        /date=2025-04-10
          /hour=14
            *.parquet
          /hour=15
            *.parquet
```
## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Read Support
As read requests come in to GigAPI they are parsed and transpiled using the GigAPI Metadata catalog to resolve data location based on database, table and timerange in requests. Series can be used with or without time ranges, ie for calculating averages, etc.

Query Data
```bash
$ curl -X POST "http://localhost:7972/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d {"query": "SELECT time, temperature FROM weather WHERE time >= epoch_ns('2025-04-24T00:00:00'::TIMESTAMP)"}
```

Series can be used with or without time ranges, ie for counting, calculating averages, etc.

```bash
$ curl -X POST "http://localhost:7972/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d '{"query": "SELECT count(*), avg(temperature) FROM weather"}'
```
```json
{"results":[{"avg(temperature)":87.025,"count_star()":"40"}]}
```

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> FlightSQL
GigAPI data can be accessed using FlightSQL GRPC clients in any language
```python
from flightsql import connect, FlightSQLClient
client = FlightSQLClient(host='localhost',port=8082,insecure=True,metadata={'bucket':'hep'})
conn = connect(client)
cursor = conn.cursor()
cursor.execute('SELECT count(*), avg(temperature) FROM weather')
print("rows:", [r for r in cursor])
```

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> GigAPI UI
The embedded GigAPI UI can be used to explore and query data using SQL with advanced features

![gigapi_preview](https://github.com/user-attachments/assets/8d550803-daa3-43dc-a4b3-b0779498fce5)


#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Grafana
GigAPI can be used from Grafana using the InfluxDB3 Flight GRPC Datasource

![image](https://github.com/user-attachments/assets/a7849ff4-b8f6-433b-8458-1c47394c5e5f)

> GigAPI readers can be implemented in any language and with any OLAP engine supporting Parquet files.

<br>

## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 />  GigAPI Diagram

```mermaid
%%{
  init: {
    'theme': 'base',
    'themeVariables': {
      'primaryColor': '#6a329f',
      'primaryTextColor': '#fff',
      'primaryBorderColor': '#7C0000',
      'lineColor': '#6f329f',
      'secondaryColor': '#006100',
      'tertiaryColor': '#fff'
    }
  }
}%%
graph TD
    subgraph "GigAPI System"
        HTTP["HTTP API"] --> DataIngestion["Data Ingestion Pipeline"]
        GRPC["GRPC API"] --> FlightSQL["FlightSQL Service"]

        Configuration["Metadata Store"] --> Storage
        Configuration --> DataIngestion
        Configuration --> Storage
        Configuration --> MergeProcess
        MergeProcess --> Configuration

        FlightSQL["FlightSQL Service"] --> Storage["Storage System"]
        FlightSQL["FlightSQL Service"] --> DuckDB["DuckDB Engine"]

        DataIngestion --> Storage["Storage System"]
        Storage --> MergeProcess["Merge Process"]
        Storage --> QueryEngine["Query Engine"]

        DuckDB["DuckDB Engine"] --> Configuration
        
        
    end
    
    Client["Client Applications"] --> HTTP
    Client["Client Applications"] --> GRPC
    
    Storage --> LocalFS["Local Filesystem"]
    Storage --> S3["S3 Storage"]
    
    QueryEngine --> DuckDB["DuckDB Engine"]    
    FlightSQL["FlightSQL Service"] --> Configuration
```


### Got Questions?
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/gigapi/gigapi)


### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors @metrico/quackpipe](https://contrib.rocks/image?repo=gigapi/gigapi)](https://github.com/gigapi/gigapi/graphs/contributors)

### Community

[![Stargazers for @metrico/quackpipe](https://reporoster.com/stars/gigapi/gigapi)](https://github.com/gigapi/gigapi/stargazers)

###### :black_joker: Disclaimers

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: InfluxDB ® is a trademark of InfluxData. No direct affiliation or endorsement. [^3]
[^4]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^4]

