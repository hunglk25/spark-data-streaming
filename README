# Spark Data Streaming

## Table of Contents
- [Introduction](#introduction)
- [System Architecture](#system-architecture)
- [Technologies](#technologies)
- [Getting Started](#getting-started)


## Introduction

This repository contains a personal project designed to enhance my skills in Data Engineering. It covers each stage from  data generation to processing and ultimately to storage on Amazon S3, leveraging a comprehensive technology stack that includes Apache Kafka for streaming, Apache Zookeeper for management, and Apache Spark for processing, all orchestrated within a Docker environment.
## System Architecture
<p align="center"> 
  <img src="image/Architecture.png" height="290">
</p>

The project is designed with the following components:

- **Data Source**: We simulate vehicle movements and generate random data representing vehicle attributes using Python scripts.
- **Apache Kafka and Zookeeper**: kafka is utilized for streaming the generated vehicle data, while Zookeeper is responsible for managing and coordinating the Kafka brokers.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Amazon S3**: Storage destination for processed data.

## Technologies

- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Amazon S3
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/hunglk25/spark-data-streaming.git
    ```
2. Navigate to the project directory:
    ```bash
    cd spark-data-streaming
    ```
3. Run virtual environment:
    ```bash
    source venv/bin/activate
    ```
4. Modify the jobs/config.py 
5. Run Docker Compose to spin up the services:
    ```bash
    docker compose up
    ```
6. Run Python file to generate and send various types of data to different Kafka topics
    ```bash
    python3 jobs/generate_data.py
    ```
    Run bash script to to list all Kafka topics available on the Kafka broker 
    ```bash
    docker exec -it broker kafka-topics --list --bootstrap-server broker:29092
    ```
    <p align="center"> 
    <img src="image/image1.png" height="200">
    </p>
7. Run the spark_submit.sh file to stream data from kafka and write to Amazon S3
    ```bash
    sudo chmod +x spark_submit.sh
    ./spark_submit.sh
    ```
    You can go access Spark's web UI at localhost:9090  to monitor and manage Spark jobs.
    <p align="center"> 
    <img src="image/image2.png" height="300">
    </p>
    You can see that the data has been saved to Amazon S3.
    <p align="center"> 
    <img src="image/image3.png" height="300">
    </p>



