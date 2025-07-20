import duckdb
from fastapi import FastAPI
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String

app = FastAPI()

Base = declarative_base()

if __name__ == "__main__":
    pass
