package es.dmr.trident.doyle.main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import es.dmr.trident.doyle.operations.KafkaDump;
import es.dmr.trident.doyle.operations.LocationExtractor;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;

/**
*
*/
public class DoyleTopLocationsDRPC {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        //conf.put("drpc.servers", Arrays.asList("127.0.0.1"));
        //conf.put("drpc.invocations.port", 3774);

        if (args.length == 6) {
            // Ready & submit the topology
            String name = args[0];
            String topic = args[1];
            String consumerId = args[2];
            BrokerHosts hosts = new ZkHosts(args[3]);
            TransactionalTridentKafkaSpout kafkaSpout = doyleSpout(hosts, topic, consumerId);

            StormSubmitter.submitTopology(name, conf, buildTopology(kafkaSpout, args[4], args[5]));

        }else{
            System.err.println("<topologyName> <topic> <consumerId> <zookeeperHost> <topicout> <brokerHost>");
        }

    }

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout, String topicOut, String broker) throws Exception {

        TridentTopology topology = new TridentTopology();

        TridentState countState = topology
                .newStream("doyle", spout)
                .each(new Fields("str"), new LocationExtractor(), new Fields("place", "count"))
                .project(new Fields("place", "count"))
                .groupBy(new Fields("place"))
                .aggregate(new Count(), new Fields("occurrences"))
                .project(new Fields("place", "occurrences"))
                .each(new Fields("place", "occurrences"), new KafkaDump(topicOut, broker))
                .groupBy(new Fields("place"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("occurrences"), new Sum(), new Fields("total"));

        // DRPC
        topology
                .newDRPCStream("top_places")
                .stateQuery(countState, new Fields("args"), new MapGet(), new Fields("individual_count"))
                .each(new Fields("individual_count"), new FilterNull());

        return topology.build();
    }

    public static TransactionalTridentKafkaSpout doyleSpout(BrokerHosts hosts, String topic, String consumerId) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic, consumerId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(kafkaConfig);
    }

}
