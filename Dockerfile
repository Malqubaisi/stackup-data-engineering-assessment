
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/app/datasets
ENV OUTPUT_DIR=/app/outputs

COPY requirements-docker.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY solutions/ /app/solutions/
COPY etl_starter.py /app/etl_starter.py

RUN mkdir -p /app/datasets /app/outputs

CMD ["python", "etl_starter.py"]
