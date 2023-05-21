Stream Processing with Apache Flink 
------------------------------------
This repository contains the code for the book **Stream Processing with Apache Flink**.


<p align="center">
    <img src="images/cover.png" width="500" height="800">
</p>


### Table of Contents
1. [Environment Setup](#environment-setup)
2. [Warmup - Connecting to Pulsar](#warmup)


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

### Warmup
**Outcomes:** How can we connect to Kafka and start consuming events.