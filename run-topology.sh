#!/usr/bin/bash

storm jar target/trident-doyle-1.0.jar es.dmr.trident.doyle.main.DoyleTopLocations doyle-topology-1 doyle-episodes storm-consumer-1 localhost:2181 storm-locations localhost:6667

