Welcome to the Real-time Change Data Capture (CDC) project! This repository contains the infrastructure setup and workflows for capturing and processing real-time data changes using a robust stack including Hadoop, Spark, Debezium, Kafka, PostgreSQL, and Flink.

Key Features:

** Infrastructure Setup:

Hadoop: Distributed file system for storing large volumes of data.
Spark: Distributed data processing engine for efficient data manipulation.
Debezium: Open-source platform for change data capture from database logs.
Kafka: Distributed streaming platform for building real-time data pipelines.
PostgreSQL: Relational database management system for storing transactional data.
Flink: Stream processing framework for high-throughput, low-latency data processing.

** Workflow 1: Spark for Change Data on HDFS:

Utilize Spark for capturing and processing change data from various sources.
Store processed data efficiently on Hadoop Distributed File System (HDFS).
Enables seamless handling of large datasets with Spark's distributed computing capabilities.

** Workflow 2: Flink for Real-time Numeric Column Computation:

Leverage Flink for real-time stream processing of numeric columns.
Compute aggregates, statistics, or other transformations on the data in real-time.
Enables instant insights and decision-making based on the continuously updating data.
