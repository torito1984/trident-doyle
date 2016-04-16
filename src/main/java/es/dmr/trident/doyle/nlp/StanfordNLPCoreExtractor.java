/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.dmr.trident.doyle.nlp;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.Triple;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 *
 * @author Danilo Radenovic
 */
public class StanfordNLPCoreExtractor{

    private AbstractSequenceClassifier<CoreLabel> classifier;
    public static final String DEFAULT_CLASSIFIER = "classifiers/english.all.3class.distsim.crf.ser.gz";
    public static final String PERSON = "PERSON";
    public static final String ORGANIZATION = "ORGANIZATION";
    public static final String LOCATION = "LOCATION";

    public StanfordNLPCoreExtractor(String pathToClassifier) throws IOException, ClassNotFoundException {
        classifier = CRFClassifier.getClassifier(pathToClassifier);
    }

    public StanfordNLPCoreExtractor() throws IOException, ClassNotFoundException {
        classifier = CRFClassifier.getClassifier(DEFAULT_CLASSIFIER);
    }

    public List<String> getPersons(String text) {
        List<String> persons = new ArrayList<>();
        List<Triple<String, Integer, Integer>> list = classifier.classifyToCharacterOffsets(text);
        for (Triple<String, Integer, Integer> item : list) {
            if (item.first().compareTo(PERSON) == 0) {
                persons.add(text.substring(item.second(), item.third()));
            }
        }
        return persons;
    }

    public List<String> getLocations(String text) {
        List<String> locations = new ArrayList<>();
        List<Triple<String, Integer, Integer>> list = classifier.classifyToCharacterOffsets(text);
        for (Triple<String, Integer, Integer> item : list) {
            if (item.first().compareTo(LOCATION) == 0) {
                locations.add(text.substring(item.second(), item.third()));
            }
        }
        return locations;
    }

    public List<String> getOrganizations(String text) {
        List<String> organizations = new ArrayList<>();
        List<Triple<String, Integer, Integer>> list = classifier.classifyToCharacterOffsets(text);
        for (Triple<String, Integer, Integer> item : list) {
            if (item.first().compareTo(ORGANIZATION) == 0) {
                organizations.add(text.substring(item.second(), item.third()));
            }
        }
        return organizations;
    }

    public Map<String, List<String>> getAll(String text) {

        Map<String, List<String>> result = new HashMap<>();
        result.put(PERSON, new LinkedList<String>());
        result.put(ORGANIZATION, new LinkedList<String>());
        result.put(LOCATION, new LinkedList<String>());
        List<Triple<String, Integer, Integer>> list = classifier.classifyToCharacterOffsets(text);
        for (Triple<String, Integer, Integer> item : list) {
            if (item.first().compareTo(LOCATION) == 0) {
                result.get(LOCATION).add(text.substring(item.second(), item.third()));
            } else if (item.first().compareTo(ORGANIZATION) == 0) {
                result.get(ORGANIZATION).add(text.substring(item.second(), item.third()));
            } else if (item.first().compareTo(PERSON) == 0) {
                result.get(PERSON).add(text.substring(item.second(), item.third()));
            }
        }
        return result;
    }

    /*
     Creates a new list of triples using the provided list of triples.
     Resulting list of triples looks like:
     <Person's name, startmarker, endmarker>
     e.g.
     <Alice Smith,5,16>
     */
    public List<Triple<String, Integer, Integer>> getPersonMarkers(String text) {
        List<Triple<String, Integer, Integer>> personsOnlyList = new ArrayList<>();
        List<Triple<String, Integer, Integer>> list = classifier.classifyToCharacterOffsets(text);
        for (Triple<String, Integer, Integer> item : list) {
            if (item.first().compareTo(PERSON) == 0) {
                String name = text.substring(item.second(), item.third());
                personsOnlyList.add(new Triple(name, item.second(), item.third()));
            }
        }
        return personsOnlyList;
    }

    public Set<String> getUniquePersons(String text) {
        return new LinkedHashSet(getPersons(text));
    }

}
