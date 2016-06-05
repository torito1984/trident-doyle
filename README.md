# Samples with Storm Trident and Sherlock Holmes stories

This code is meant to be run in connection with a Kafka producer (see https://github.com/torito1984/kafka-doyle-generator.git).
It supposes that a Kafka topic is populated with pieces of text. It includes 2 examples:

-  DoyleTopLocations: It extracts a set of locations with Stanford NLP from the texts and publishes an updated count of the aggregates 
of occurences. It uses Trident to do the aggregation and the count is published back to a different Kafka topic so it could 
used by further processes.
- DoyleTopLocationsDRPC: This example keeps the same count and allows to query specific values with Trident DRPC capabilities.

In order to run the examples:

- runTopology.sh to run the first example.
- run-topology-drpc.sh to run the DRPC example.

Once the DRPC topoogy is running, you can query at any time using run-client.sh

The code has been tested with Kafka 0.9.0.4 included in Hortonworks HDP 2.4.0. It supposes that Kafka is available in 
localhost:6667 and Zookeeper in localhost:2181. These locations can be configured in the scripts. 
