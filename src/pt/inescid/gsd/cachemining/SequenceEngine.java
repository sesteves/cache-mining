package pt.inescid.gsd.cachemining;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class SequenceEngine {

    public final static String SEPARATOR = ":";

    private static final String FILENAME = "sequences.txt";

    private Logger log = Logger.getLogger(SequenceEngine.class);

    private Map<String, Set<String>> sequences = new HashMap<String, Set<String>>();

    public SequenceEngine() {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        loadSequences();
    }

    private void loadSequences() {

        try {
            BufferedReader br = new BufferedReader(new FileReader(FILENAME));
            String line;
            int countSequences = 0;
            while ((line = br.readLine()) != null) {

                String[] items = line.substring(0, line.lastIndexOf(':') - 1).split(" ");
                countSequences += items.length;

                if (sequences.containsKey(items[0])) {
                    Set<String> itemsSet = sequences.get(items[0]);
                    itemsSet.addAll(Arrays.asList(items));
                } else {
                    sequences.put(items[0], new HashSet<String>(Arrays.asList(items)));
                }

            }
            br.close();

            log.info("Loaded " + countSequences + " sequences indexed by " + sequences.size() + " indexes.");

        } catch (FileNotFoundException e) {
            log.fatal(e.getMessage());
        } catch (IOException e) {
            log.fatal(e.getMessage());
        }
    }

    public Set<String> getSequence(String key) {
        return sequences.get(key);
    }

    public static void main(String[] args) {
        SequenceEngine engine = new SequenceEngine();
        engine.loadSequences();
    }

}
