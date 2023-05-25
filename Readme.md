Stream Processing with Apache Flink 
------------------------------------

<p align="center">
    <img src="assets/cover.png" width="500" height="1200">
</p>

This repository contains the code for the book **[Stream Processing: Hands-n with Apache Flink](https://leanpub.com/streamprocessingwithapacheflink)**.


### Table of Contents
1. [Environment Setup](#environment-setup)
2. [Register UDF](#register-udf)


### Environment Setup
In order to run the code samples we will need a Kafka and Flink cluster up and running.
You can also run the Flink examples from within your favorite IDE in which case you don't need a Flink Cluster.

If you want to run the examples inside a Flink Cluster run to start the Pulsar and Flink clusters.
```shell
docker-compose up
```

When the cluster is up and running successfully run the following command:
```shell
./setup.sh
```

### Register UDF
```shell
CREATE FUNCTION maskfn  AS 'io.streamingledger.udfs.MaskingFn'  LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';
CREATE FUNCTION splitfn AS 'io.streamingledger.udfs.SplitFn'    LANGUAGE JAVA USING JAR '/opt/flink/jars/spf-0.1.0.jar';

CREATE TEMPORARY VIEW sample AS
SELECT * 
FROM transactions 
LIMIT 10;

SELECT transactionId, maskfn(UUID()) AS maskedCN FROM sample;
SELECT transactionId, operation, word, length FROM sample, LATERAL TABLE(splitfn(operation));
```
