package es.dmr.trident.doyle.operations;

import backtype.storm.tuple.Values;
import es.dmr.trident.doyle.nlp.NLPConceptExtractor;
import org.apache.commons.lang.StringUtils;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * Function that extracts locations from a piece of text
 *
 */
@SuppressWarnings("serial")
public class LocationExtractor extends BaseFunction {

    private static final long serialVersionUID = 1L;
    private NLPConceptExtractor extractor;
    private boolean loaded = false;

    public LocationExtractor() throws Exception {
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        try {
            if(!loaded) {
                extractor = new NLPConceptExtractor();
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
