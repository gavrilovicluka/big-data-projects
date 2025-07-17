@echo off
docker build --rm -t bde/spark-app .
docker run --name flightDelays --net bde -p 4040:4040 bde/spark-app