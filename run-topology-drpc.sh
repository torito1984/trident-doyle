#!/usr/bin/bash

storm jar target/trident-doyle-1.0.jar es.dmr.trident.doyle.main.DoyleTopLocationsDRPC doyle-topology doyle-episodes storm-consumer localhost:2181 storm-locations localhost:6667

