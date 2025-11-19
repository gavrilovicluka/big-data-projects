# Spark MLlib

This project presents a system for Big Data analysis and machine learning (ML) in a distributed environment, combining off-line model training and on-line real-time prediction (Structured Streaming).

The project is divided into four main phases, implemented in separate Docker services: 
- ML Model Training (**PySpark MLlib**)
- Reading CSV Data and Sending to Kafka Topic (**Kafka**)
- Stream Processing, Prediction and Writing Results to CSV (**Spark Structured Streaming**)
- Visualization (**Jupyter/GeoPandas**)