FROM python:3.12-bookworm
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY . .
# TODO: RUN go generate
CMD ["python", "__main__.py"]
