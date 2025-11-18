# 🗂️ Big Data Systems

This reporistory contains three projects developed for the **Big Data Systems** course as part of **Master Academic Studies**, **Artificial Intelligence and Machine Learning** module, at the **Faculty of Electronic Engineering, University of Niš**. 

Together, these projects form a complete system for large scale batch processing, real-time prediction, and visualization of flight delays using Apache Spark, Kafka, and Jupyter Notebook.

The system is built around the [2019 Airline Delays w/Weather and Airport Detail](https://www.kaggle.com/datasets/threnjen/2019-airline-delays-and-cancellations?select=full_data_flightdelay.csv) dataset, and includes distributed preprocessing, model training, streaming ingestion, prediction pipelines, and analytical visualization.

All components run inside Docker containers, and Spark is used in both standalone mode and Structured Streaming, while data storage relies on the local filesystem or HDFS depending on configuration.