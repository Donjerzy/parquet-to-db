FROM python:latest

RUN pip install pyarrow pandas sqlalchemy psycopg2

WORKDIR /app

COPY parquet parquet
COPY main.py main.py

ENTRYPOINT ["python", "main.py"]