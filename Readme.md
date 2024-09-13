# Traffic Data Streaming and Analysis with PySpark and Cassandra

## Overview

This project is a **real-time traffic data streaming** and **batch analysis** solution, leveraging **Apache PySpark** for real-time data processing and **Apache Cassandra** for storing the aggregated results. The data consists of per-vehicle traffic records captured from different lanes, including details like vehicle type, speed, and other attributes.

The project streams data, performs real-time transformations, and stores summarized insights such as vehicle counts, speed averages, and lane-specific traffic analysis.

## Features

- **Data Ingestion**: Stream traffic data continuously from CSV files.
- **Real-time Processing**: Use PySpark Streaming to process traffic data in near real-time.
- **Cassandra Integration**: Store processed and aggregated results in Apache Cassandra for further analysis.
- **Traffic Insights**: Capture important insights such as vehicle counts, average speed, and lane-wise traffic flow.
- **Time-based Batch Processing**: The data is processed in 5-second intervals to generate continuous traffic insights.

## Requirements

- **Python 3.x**
- **Apache Spark (PySpark)**
- **Apache Cassandra**
- **Hadoop (optional for HDFS)**

## Setup Instructions

### Prerequisites

1. **Apache Spark**: Install Apache Spark and configure it to work with PySpark.
2. **Apache Cassandra**: Install Apache Cassandra and create the required keyspaces and tables.
   ```cql
   CREATE KEYSPACE streams WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
   CREATE TABLE streams.q1 (time TEXT, classname TEXT, count INT, PRIMARY KEY (time, classname));
   CREATE TABLE streams.q2 (time TEXT, classname TEXT, avg_speed FLOAT, PRIMARY KEY (time, classname));
   CREATE TABLE streams.q3 (time TEXT, site TEXT, count INT, PRIMARY KEY (time, site));
   CREATE TABLE streams.q4 (time TEXT, m50parts TEXT, count INT, PRIMARY KEY (time, m50parts));
   ```
3. **Hadoop (optional)**: If you're using HDFS, make sure to have it installed and configured.

### Installing Dependencies

```bash
pip install pyspark cassandra-driver
```

### Running the Application

1. **Streaming CSV Files**: The following script will split a large CSV file into smaller parts and write them to the streaming directory. Each part is written every 5 seconds to simulate live data streaming:
   
   ```python
   input = (line.rstrip('\n') for line in open(inputPath))
   input = islice(input, 1, None)  # Skips the header
   inputpart = split_part(input, 10)  # Split into parts of 10 records each
   for part in inputpart:
       save_file(part, '{}/countdata{}.csv'.format(outputPath, read))
       time.sleep(5)  # Simulate live streaming
   ```

2. **Starting PySpark Streaming**: After splitting the CSV files, you can start the PySpark Streaming job:
   
   ```bash
   spark-submit traffic_streaming.py
   ```

## Real-Time Traffic Queries

### Query 1: Vehicle Count by Type
The first query processes the stream and counts the number of vehicles by type in each batch. The results are saved to the Cassandra table `q1`.

```python
q1 = records.map(lambda x: ((x[14]), 1)).reduceByKey(lambda a, b: a + b)
```

### Query 2: Average Speed by Vehicle Type
The second query calculates the average speed for each vehicle type in real-time. The results are saved to the Cassandra table `q2`.

```python
q2 = records.map(lambda x: (x[14], float(x[18]))).groupByKey().map(lambda x: (x[0], sum(x[1]) / len(x[1])))
```

### Query 3: Traffic Count by Location
The third query counts the number of vehicles passing through different sites or junctions in real-time. The results are saved to the Cassandra table `q3`.

```python
q3 = records.map(lambda x: ((x[0]), 1)).reduceByKey(lambda x, y: x + y)
```

### Query 4: Lane-Specific Traffic Analysis for HGV Vehicles
The fourth query filters for HGV (Heavy Goods Vehicles) and counts their distribution across lanes. The results are saved to the Cassandra table `q4`.

```python
q4 = records.filter(lambda x: "HGV" in x[14]).map(lambda x: (x[10], 1)).reduceByKey(lambda x, y: x + y)
```

## Key Components

- **CSV File Streaming**: The CSV data is split into parts and streamed into PySpark Streaming for real-time processing.
- **PySpark Streaming**: The main engine for real-time data processing. It processes batches of data every 5 seconds.
- **Apache Cassandra**: Used for storing the aggregated results and insights from the streaming data.

## Data Flow

1. **CSV Input**: Data is ingested from large CSV files, split into smaller chunks, and streamed to PySpark.
2. **Real-time Processing**: The data is processed in 5-second intervals using PySpark's StreamingContext.
3. **Cassandra Storage**: Aggregated data such as vehicle counts, average speed, and lane-wise traffic is stored in Cassandra for further analysis.

## Results

The results of each query can be viewed in the Cassandra tables:
- `q1`: Vehicle counts by type
- `q2`: Average speed by vehicle type
- `q3`: Traffic counts by site/junction
- `q4`: Lane-specific HGV traffic analysis

## License

This project is licensed under the MIT License.
