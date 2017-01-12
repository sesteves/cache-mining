package pt.inescid.gsd.cachemining;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class SequenceEngine {

	private final static String PROPERTIES_FILE = "cachemining.properties";
	
	private final static String SEQUENCES_FILE_KEY = "sequencesFile";
	private final static String SEQUENCES_FILE_DEFAULT = "sequences.txt";
	
	
    public final static String SEPARATOR = ":";

    private Logger log = Logger.getLogger(SequenceEngine.class);

    // private Map<String, List<String>> sequences = new HashMap<>();

    private Map<String, Node> sequences = new HashMap<>();

	private String sequencesFile;

    public SequenceEngine() {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));            
        } catch (IOException e) {
            log.info("Not possible to load properties file '" + PROPERTIES_FILE + "'.");
        }
        sequencesFile = properties.getProperty(SEQUENCES_FILE_KEY, SEQUENCES_FILE_DEFAULT);
        
        loadSequences();
    }

    public SequenceEngine(Map<String, List<String>> sequences) {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            log.info("Not possible to load properties file '" + PROPERTIES_FILE + "'.");
        }

        this.sequences = sequences;
        log.info("Loaded " + sequences.size() + " sequences");
    }

    private void loadSequences() {

        try {
            BufferedReader br = new BufferedReader(new FileReader(sequencesFile));
            String line;
            int countSequences = 0;
            while ((line = br.readLine()) != null) {

                String[] items = line.substring(0, line.lastIndexOf(':') - 1).split(" ");
                countSequences += items.length;

                if (sequences.containsKey(items[0])) {
                    List<String> itemsSet = sequences.get(items[0]);
                    itemsSet.addAll(Arrays.asList(items));
                } else {
                    sequences.put(items[0], new ArrayList<>(Arrays.asList(items)));
                }

            }
            br.close();

            log.info("Loaded " + countSequences + " sequences indexed by " + sequences.size() + " indexes from file " + sequencesFile);

        } catch (FileNotFoundException e) {
            log.fatal(e.getMessage());
        } catch (IOException e) {
            log.fatal(e.getMessage());
        }
    }

    public List<String> getSequence(String key) {
        return sequences.get(key);
    }

    public static void main(String[] args) {
        SequenceEngine engine = new SequenceEngine();
        engine.loadSequences();
    }

    private class Node {

        private String value;

        private Map<Double, Node> children;

        public Node(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

}
