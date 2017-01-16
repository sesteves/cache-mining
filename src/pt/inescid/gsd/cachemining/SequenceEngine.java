package pt.inescid.gsd.cachemining;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

public class SequenceEngine {

	private final static String PROPERTIES_FILE = "cachemining.properties";
	
	private final static String SEQUENCES_FILE_KEY = "sequencesFile";
	private final static String SEQUENCES_FILE_DEFAULT = "sequences.txt";
	
	
    public final static String SEPARATOR = ":";

    private Logger log = Logger.getLogger(SequenceEngine.class);

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
        
        // loadSequences();
    }

    public SequenceEngine(List<List<String>> sequences) {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            log.info("Not possible to load properties file '" + PROPERTIES_FILE + "'.");
        }

        // load sequences
        for(List<String> sequence : sequences) {
            Node parent = null;
            for(int i = 0; i < sequence.size(); i++) {
                String item = sequence.get(i);
                if (i == 0) {
                    if(this.sequences.containsKey(item)) {
                        parent = this.sequences.get(item);
                    } else {
                        parent = new Node(item);
                        this.sequences.put(item, parent);
                    }
                } else {
                    Node node = new Node(item);
                    parent.addChild(node, 1.0);
                }
            }
        }

        log.info("Loaded " + sequences.size() + " sequences");
    }

//    private void loadSequences() {
//
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(sequencesFile));
//            String line;
//            int countSequences = 0;
//            while ((line = br.readLine()) != null) {
//
//                String[] items = line.substring(0, line.lastIndexOf(':') - 1).split(" ");
//                countSequences += items.length;
//
//                if (sequences.containsKey(items[0])) {
//                    List<String> itemsSet = sequences.get(items[0]);
//                    itemsSet.addAll(Arrays.asList(items));
//                } else {
//                    sequences.put(items[0], new ArrayList<>(Arrays.asList(items)));
//                }
//
//            }
//            br.close();
//
//            log.info("Loaded " + countSequences + " sequences indexed by " + sequences.size() + " indexes from file " + sequencesFile);
//
//        } catch (FileNotFoundException e) {
//            log.fatal(e.getMessage());
//        } catch (IOException e) {
//            log.fatal(e.getMessage());
//        }
//    }

    public Iterator<String> getSequences(String key) {
        Node root = sequences.get(key);
        if(root == null) {
            return null;
        }
        return new SequenceIterator(sequences.get(key));
    }

    public static void main(String[] args) {
        SequenceEngine engine = new SequenceEngine();
    }

    /**
     * Currently searches in BFS way
     *
     * TODO: add dummy node to check for depth
     */
    private class SequenceIterator implements Iterator<String> {

        private static final int DEPTH = 100;

        private Node parent;

        private Node currentNode = null;

        private int currentChild = 0;

        private int currentDepth;

        private Queue<Node> queue = new LinkedList<>();

        public SequenceIterator(Node root) {
            parent = root;
            if(parent.children != null) {
                currentNode = parent.children.get(currentChild);
                queue.add(currentNode);
            }
        }

        @Override
        public boolean hasNext() {
            return currentNode != null;
        }

        @Override
        public String next() {
            Node result = currentNode;

            if(parent.children.size() - 1 == currentChild) {
                if(!queue.isEmpty()) {
                    parent = queue.poll();
                    currentChild = 0;
                    currentNode = parent.children.get(currentChild);
                } else {
                    currentNode = null;
                }
            } else {
                currentNode = parent.children.get(++currentChild);
                if(currentNode.children != null) {
                    queue.add(currentNode);
                }
            }

            return result.value;
        }

        @Override
        public void remove() {
            if(hasNext()) {
                next();
            }
        }
    }


    private class Node {

        private String value;

        private List<Node> children;

        public Node(String value) {
            this.value = value;
        }

        public void addChild(Node node, double probability) {
            if(children == null) {
                children = new ArrayList<>();
            }
            children.add(node);
        }
    }

}
