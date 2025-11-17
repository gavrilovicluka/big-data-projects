FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY cassandra-writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cassandra-writer/cassandra_writer.py .

CMD ["python", "cassandra_writer.py"]
