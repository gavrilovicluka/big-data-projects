FROM python:3.10-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py .

CMD python -u $PRODUCER_SCRIPT