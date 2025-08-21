FROM python:3.12-bookworm
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-linux-amd64.zip &&  \
    unzip duckdb_cli-linux-amd64.zip && ls
COPY . .
RUN bash -c 'CMD=setup python __main__.py'
# TODO: RUN go generate
CMD ["python", "__main__.py"]
