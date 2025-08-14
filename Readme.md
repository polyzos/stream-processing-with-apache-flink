Stream Processing with Apache Flink 
------------------------------------

<p align="center">
    <img src="assets/cover.png" width="600" height="800">
</p>

This repository contains the code for the book **[Stream Processing: Hands-on with Apache Flink](https://leanpub.com/streamprocessingwithapacheflink)**.

> **🚀 Project Status: Ready for Streaming Processing!**
> 
> ✅ **Infrastructure**: RedPanda (Kafka) + Flink cluster running  
> ✅ **Project**: Compiled and packaged  
> ✅ **Data Loaded**: 1M+ transactions, 5K+ customers, 4K+ accounts in Kafka topics  
> ✅ **Next Step**: Ready to run Flink streaming jobs


### Table of Contents
1. [Environment Setup](#environment-setup)
2. [Project Setup and Data Loading](#project-setup-and-data-loading)
3. [Register UDF](#register-udf)
4. [Deploy a JAR file](#deploy-a-jar-file)


### Environment Setup
In order to run the code samples we will need a Kafka and Flink cluster up and running.
You can also run the Flink examples from within your favorite IDE in which case you don't need a Flink Cluster.

If you want to run the examples inside a Flink Cluster run the following command to start the services.
```shell
docker-compose up
```

When the cluster is up and running successfully run the following command for redpanda:
```shell
./redpanda-setup.sh

```

or this command for kafka setup
```shell
./kafka-setup.sh
```


### Project Setup and Data Loading
This section documents the steps to compile the project and load data into Kafka for streaming processing.

#### 1. Compile the Project
The project is a Maven-based Java application. To compile it:

```shell
# Compile only
mvn clean compile

# Compile and create JAR file
mvn clean package
```

#### 2. Load Data into Kafka
After starting the infrastructure (RedPanda/Kafka + Flink), you need to populate the topics with data:

**Run Transactions Producer:**
```shell
mvn exec:java -Dexec.mainClass="io.streamingledger.producers.TransactionsProducer"
```
This will:
- Load 1,000,000 transactions from `/data/transactions.csv`
- Send them to the `transactions` topic
- Use optimized producer settings (batch size: 64KB, compression: gzip)

**Run State Producer:**
```shell
mvn exec:java -Dexec.mainClass="io.streamingledger.producers.StateProducer"
```
This will:
- Load 5,369 customers from `/data/customers.csv` → `customers` topic
- Load 4,500 accounts from `/data/accounts.csv` → `accounts` topic

#### 3. Verify Data Loading
After running both producers, you should have:
- **`transactions`** topic: 1,000,000 transaction records
- **`customers`** topic: 5,369 customer records  
- **`accounts`** topic: 4,500 account records

You can monitor the data flow through RedPanda Console at `http://localhost:8080`

#### 4. Alternative Ways to Run
```shell
# Option 1: Using Maven exec plugin (recommended)
mvn exec:java -Dexec.mainClass="io.streamingledger.producers.TransactionsProducer"

# Option 2: Using the compiled JAR
java -cp target/classes:target/dependency/* io.streamingledger.producers.TransactionsProducer

# Option 3: Using the packaged JAR
java -jar target/spf-0.1.0.jar
```


### Register UDF
```shell
CREATE FUNCTION maskfn  AS 'io.streamingledger.udfs.MaskingFn'      LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION splitfn AS 'io.streamingledger.udfs.SplitFn'        LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION lookup  AS 'io.streamingledger.udfs.AsyncLookupFn'  LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';

CREATE TEMPORARY VIEW sample AS
SELECT * 
FROM transactions 
LIMIT 10;

SELECT transactionId, maskfn(UUID()) AS maskedCN FROM sample;
SELECT * FROM transactions, LATERAL TABLE(splitfn(operation));

SELECT 
  transactionId,
  serviceResponse, 
  responseTime 
FROM sample, LATERAL TABLE(lookup(transactionId));
```

### Deploy a JAR file
1. Package the application and create an executable jar file
```shell
mvn clan package
```
2. Copy it under the jar files to be included in the custom Flink images

3. Start the cluster to build the new images by running
```shell
docker-compose up
```

4. Deploy the flink job
```shell
docker exec -it jobmanager ./bin/flink run \
  --class io.streamingledger.datastream.BufferingStream \
  jars/spf-0.1.0.jar
```
