FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY stream/requirements.txt stream/requirements.txt
RUN pip install --no-cache-dir -r stream/requirements.txt

COPY stream stream
