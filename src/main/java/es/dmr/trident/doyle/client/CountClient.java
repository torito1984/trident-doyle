package es.dmr.trident.doyle.client;

import backtype.storm.Config;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

/**
 * Created by osboxes on 4/16/16.
 */
public class CountClient {

    public static void main(String[] args){
        try {
            if(args.length == 2) {
                Config conf = new Config();
                conf.setDebug(true);
                conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
                conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
                conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
                conf.put(Config.DRPC_MAX_BUFFER_SIZE, 1548576000);
                conf.put("storm.thrift.transport", "backtype.storm.security.auth.SimpleTransportPlugin");

                DRPCClient client = new DRPCClient(conf, args[0], Integer.parseInt(args[1]), 10000);

                while (true) {
                    String result = client.execute("top_places", "United States");
                    System.out.println("United States occurences: " + result);
                    Thread.sleep(5000);
                }
            }else{
                System.out.println("Usage: <host> <port>");
            }
        } catch (TException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
