FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD python -u $PRODUCER_SCRIPT


# FROM python:3.8-slim
# WORKDIR /
# RUN pip install kafka-python==2.0.2
# COPY . .
# CMD python -u $SCRIPT