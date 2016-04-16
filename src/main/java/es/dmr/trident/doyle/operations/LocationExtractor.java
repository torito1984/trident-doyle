package es.dmr.trident.doyle.operations;

import backtype.storm.tuple.Values;
import es.dmr.trident.doyle.nlp.StanfordNLPCoreExtractor;
import org.apache.commons.lang.StringUtils;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;


/**
 * Dummy function that just emits the uppercased text.
 */
@SuppressWarnings("serial")
public class LocationExtractor extends BaseFunction {

    private static final long serialVersionUID = 1L;
    private StanfordNLPCoreExtractor extractor;
    private boolean loaded = false;

    public LocationExtractor() throws Exception {
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        try {
            if(!loaded) {
                extractor = new StanfordNLPCoreExtractor();
                loaded = true;
            }

            String text = tuple.getString(0);
            // emit the locations detected
            if(!StringUtils.isEmpty(text))
                extractor.getLocations(text).stream()
                        .forEach(place ->
                                collector.emit(new Values(place, 1)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
