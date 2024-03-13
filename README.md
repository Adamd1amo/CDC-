# <strong>Real-time Change Data Capture (CDC)<strong>

Welcome to the Real-time Change Data Capture (CDC) project! This repository hosts the infrastructure setup and workflows for capturing and processing real-time data changes using a robust stack of technologies, including Hadoop, Spark, Debezium, Kafka, PostgreSQL, and Flink.

![image](https://github.com/Adamd1amo/CDC-/assets/61895816/061b4727-5be1-457b-9d0a-20a90d4e4f56)


## Key Features:

### Infrastructure Setup:
Docker (Current) -> Docker Swarm -> Mini K8s -> K8s
- **Hadoop**: A distributed file system for storing large volumes of data.
- **Delta lake**: A storage layer provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on top of existing data lakes
- **Debezium**: An open-source platform for change data capture from database logs.
- **Kafka**: A distributed streaming platform for building real-time data pipelines.
- **PostgreSQL**: A relational database management system for storing transactional data.
- **Flink**: A stream processing framework for high-throughput, low-latency data processing.
- **Spark**: A distributed data processing engine for efficient data manipulation.

Is considering:
- **Memory caching**: Redis or Hazelcast


### Workflow 1: Spark for Change Data on HDFS:

#### Current workflow:

![image](https://github.com/Adamd1amo/CDC-/assets/61895816/3176207b-20e9-4ebb-b70a-e98990fa75b0)


- Utilize Spark for capturing and processing change data from Postgres.
- Mapping data using metadata of table stored on HDFS or infer schema use schema_of_json and from_json functions
- Store processed data efficiently on the Hadoop Distributed File System (HDFS).

Next:
- Exactly-once writing data
- Handling the failure of committing offsets

### Workflow 2: Flink for Real-time Numeric Column Computation:

- Leverage Flink for real-time stream processing of numeric columns.
- Compute aggregates, statistics, or other transformations on the data in real-time.
- Enables instant insights and decision-making based on the continuously updating data.
