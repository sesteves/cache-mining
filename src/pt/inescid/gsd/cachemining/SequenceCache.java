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

public class SequenceCache {

    private static final String filename = "sequences.txt";

    private Map<String, Set<String>> sequences = new HashMap<String, Set<String>>();

    private Map<String, CacheEntry> cache = new HashMap<String, CacheEntry>();

    private void loadMetadata() {

        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = br.readLine()) != null) {

                String[] items = line.substring(0, line.lastIndexOf(':') - 1).split(" ");

                if (sequences.containsKey(items[0])) {
                    Set<String> itemsSet = sequences.get(items[0]);
                    itemsSet.addAll(Arrays.asList(items));
                } else {
                    sequences.put(items[0], new HashSet<String>(Arrays.asList(items)));
                }

            }
            br.close();

            // String str = "";
            // for (String s : sequences.get("WAREHOUSE:2:h")) {
            // str += s + " - ";
            // }
            // System.out.println("Sequence -> " + str);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CacheEntry get(String key) {
        CacheEntry entry = cache.get(key);

        return null;
    }

    public void put(String key, CacheEntry entry) {
        cache.put(key, entry);
    }

    public Set<String> getSequence(String key) {
        return sequences.get(key);
    }

    public static void main(String[] args) {
        SequenceCache cache = new SequenceCache();
        cache.loadMetadata();

    }
}
