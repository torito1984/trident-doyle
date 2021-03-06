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
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

/**
*
*/
public class DoyleTopLocations {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

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

        topology
                .newStream("doyle", spout)
                .each(new Fields("str"), new LocationExtractor(), new Fields("place", "count"))
                .project(new Fields("place", "count"))
                .groupBy(new Fields("place"))
                .aggregate(new Count(), new Fields("occurrences"))
                .project(new Fields("place", "occurrences"))
                .each(new Fields("place", "occurrences"), new KafkaDump(topicOut, broker));

        return topology.build();
    }

    public static TransactionalTridentKafkaSpout doyleSpout(BrokerHosts hosts, String topic, String consumerId) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic, consumerId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(kafkaConfig);
    }

}
