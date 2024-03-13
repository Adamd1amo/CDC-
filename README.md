# Real-time Change Data Capture (CDC)

Welcome to the Real-time Change Data Capture (CDC) project! This repository hosts the infrastructure setup and workflows for capturing and processing real-time data changes using a robust stack of technologies, including Hadoop, Spark, Debezium, Kafka, PostgreSQL, and Flink.

## Key Features:

### Infrastructure Setup:

- **Hadoop**: A distributed file system for storing large volumes of data.
- **Spark**: A distributed data processing engine for efficient data manipulation.
- **Debezium**: An open-source platform for change data capture from database logs.
- **Kafka**: A distributed streaming platform for building real-time data pipelines.
- **PostgreSQL**: A relational database management system for storing transactional data.
- **Flink**: A stream processing framework for high-throughput, low-latency data processing.

### Workflow 1: Spark for Change Data on HDFS:

- Utilize Spark for capturing and processing change data from various sources.
- Store processed data efficiently on the Hadoop Distributed File System (HDFS).
- Enables seamless handling of large datasets with Spark's distributed computing capabilities.

### Workflow 2: Flink for Real-time Numeric Column Computation:

- Leverage Flink for real-time stream processing of numeric columns.
- Compute aggregates, statistics, or other transformations on the data in real-time.
- Enables instant insights and decision-making based on the continuously updating data.