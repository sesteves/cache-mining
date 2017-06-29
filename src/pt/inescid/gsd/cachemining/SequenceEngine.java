package pt.inescid.gsd.cachemining;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import pt.inescid.gsd.cachemining.heuristics.FetchAll;
import pt.inescid.gsd.cachemining.heuristics.FetchProgressively;
import pt.inescid.gsd.cachemining.heuristics.FetchTopN;
import pt.inescid.gsd.cachemining.heuristics.Heuristic;

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

    public enum HeuristicEnum {
        FETCH_ALL, FETCH_TOP_N, FETCH_PROGRESSIVELY;

        public static HeuristicEnum getHeuristicEnum(String s) {
            switch (s) {
                case "fetch-all": return FETCH_ALL;
                case "fetch-top-n": return FETCH_TOP_N;
                case "fetch-progressively": return FETCH_PROGRESSIVELY;
                default: return null;
            }
        }
    }

	private final static String PROPERTIES_FILE = "cachemining.properties";
	
	private final static String SEQUENCES_FILE_KEY = "sequencesFile";
	private final static String SEQUENCES_FILE_DEFAULT = "sequences.txt";

    private final static String HEURISTIC_KEY = "heuristic";

    private Logger log = Logger.getLogger(SequenceEngine.class);

    private Map<DataContainer, Node> sequences = new HashMap<>();

	private String sequencesFile;

    HeuristicEnum heuristic;

    /**
     * Create a SequenceEngine with sequences read from a file.
     */
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

    /**
     * Create a SequenceEngine with a given list of sequences.
     *
     * @param sequences the sequences to be used
     */
    public SequenceEngine(List<List<DataContainer>> sequences) {
        PropertyConfigurator.configure("cachemining-log4j.properties");

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(PROPERTIES_FILE));
        } catch (IOException e) {
            log.info("Not possible to load properties file '" + PROPERTIES_FILE + "'.");
        }

        String heuristicStr = properties.getProperty(HEURISTIC_KEY);
        heuristic = HeuristicEnum.getHeuristicEnum(heuristicStr);

        // load sequences
        for(List<DataContainer> sequence : sequences) {
            Node parent = null;
            for(int i = 0; i < sequence.size(); i++) {
                DataContainer item = sequence.get(i);
                if (i == 0) {
                    if(this.sequences.containsKey(item)) {
                        parent = this.sequences.get(item);
                    } else {
                        parent = new Node(item);
                        this.sequences.put(item, parent);
                    }
                } else {
                    Node node = parent.getChild(item);
                    if (node == null) {
                        node = new Node(item);
                        parent.addChild(node, 1.0);
                    }
                    parent = node;
                }
            }
        }

        // add special nodes at the end of each level
        for (Node root : this.sequences.values()) {
            Node node = root;
            while(node.getChildren() != null) {
                node.addChild(new Node(),1);
                node = node.getChildren().get(node.getChildren().size() - 2);
            }
        }

        log.info("Loaded " + sequences.size() + " sequences. Heuristic in use: " + heuristicStr);
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

    public Heuristic getSequences(DataContainer key) {
        Node root = sequences.get(key);
        if(root == null) {
            return null;
        }
//        return new SequenceIterator(root);

        switch(heuristic) {
            case FETCH_ALL: return new FetchAll(root);
            case FETCH_TOP_N: return new FetchTopN(root);
            case FETCH_PROGRESSIVELY: return new FetchProgressively(root);
            default: return null;
        }
    }

    public static void main(String[] args) {
        SequenceEngine engine = new SequenceEngine();
    }

    /**
     * SequenceIterator - currently searches in BFS way
     */
//    private class SequenceIterator implements Iterator<DataContainer> {
//
//        private static final int MAX_DEPTH = 100;
//
//        private Node parent;
//
//        private Node currentNode = null;
//
//        private int currentChild = 0;
//
//        private int currentDepth;
//
//        private Queue<Node> queue = new LinkedList<>();
//
//        public SequenceIterator(Node root) {
//            parent = root;
//            if(parent.children != null) {
//                currentNode = parent.children.get(currentChild);
//                queue.add(currentNode);
//                currentDepth = 1;
//            }
//        }
//
//        @Override
//        public boolean hasNext() {
//            return currentNode != null;
//        }
//
//        @Override
//        public DataContainer next() {
//            Node result = currentNode;
//
//            do {
//                if (parent.children.size() - 1 == currentChild) {
//                    if (!queue.isEmpty()) {
//                        parent = queue.poll();
//                        currentChild = 0;
//                        currentNode = parent.children.get(currentChild);
//                        if(currentNode.children != null) {
//                            queue.add(currentNode);
//                        }
//                    } else {
//                        currentNode = null;
//                        break;
//                    }
//                } else {
//                    currentNode = parent.children.get(++currentChild);
//                    if (currentNode.value == null) {
//                        currentDepth++;
//                    } else if (currentNode.children != null) {
//                        queue.add(currentNode);
//                    }
//                }
//            } while (currentNode.value == null);
//
//            return result.value;
//        }
//
//        @Override
//        public void remove() {
//            if(hasNext()) {
//                next();
//            }
//        }
//    }

}
