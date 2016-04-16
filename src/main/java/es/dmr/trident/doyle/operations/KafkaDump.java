package es.dmr.trident.doyle.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author David Martinez
 */
public class KafkaDump extends BaseFilter {
    private String topic;
    private String host;
    private boolean loaded = false;
    private Producer<String, String> producer;

    public KafkaDump(String topic, String host) {
        this.topic = topic;
        this.host = host;
    }


    @Override
    public boolean isKeep(TridentTuple tuple) {

        if(!loaded){
            Properties props = new Properties();

            props.put("metadata.broker.list", host);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");
            producer = new Producer<String, String>(new ProducerConfig(props));
        }

        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, tuple.getString(0), "{ 'place':'" + tuple.getString(0) + "', 'count':"+ tuple.getInteger(1) +"'}");
        producer.send(data);
        return true;
    }
}