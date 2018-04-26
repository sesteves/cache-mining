package pt.inescid.gsd.cachemining;

import ca.pfv.spmf.algorithms.sequentialpatterns.spam.AlgoSPAM;
import ca.pfv.spmf.algorithms.sequentialpatterns.spam.AlgoVMSP;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import pt.inescid.gsd.cachemining.heuristics.FetchAll;
import pt.inescid.gsd.cachemining.heuristics.FetchProgressively;
import pt.inescid.gsd.cachemining.heuristics.FetchTopN;
import pt.inescid.gsd.cachemining.heuristics.Heuristic;
import sun.plugin2.main.server.ClientJVMSelectionParameters;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.inescid.gsd.cachemining.SequenceEngine.HeuristicEnum.FETCH_PROGRESSIVELY;

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

    private Logger log = Logger.getLogger(SequenceEngine.class);

    private Map<DataContainer, Node> sequences = new HashMap<>();

    HeuristicEnum heuristic;

    private List<PrefetchingContext> activeContexts = new ArrayList<>();

    /**
     * Create a SequenceEngine with sequences read from a file.
     */
    public SequenceEngine(String heuristicStr, String sequencesFName) {
        PropertyConfigurator.configure("cachemining-log4j.properties");
        heuristic = HeuristicEnum.getHeuristicEnum(heuristicStr);
        loadSequences(sequencesFName);
    }

    /**
     * Create a SequenceEngine with a given list of sequences.
     *
     * @param sequences the sequences to be used
     */
    public SequenceEngine(String heuristicStr, List<List<DataContainer>> sequences) {
        PropertyConfigurator.configure("cachemining-log4j.properties");
        heuristic = HeuristicEnum.getHeuristicEnum(heuristicStr);
        loadSequences(sequences);
    }

    /**
     * Loads sequences from a list.
     *
     * @param sequences the sequences to be used
     */
    private void loadSequences(List<List<DataContainer>> sequences) {
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

        log.info("Loaded " + sequences.size() + " sequences. Heuristic in use: " + heuristic);
    }


    private void loadSequences(String sequencesFName) {
        List<List<DataContainer>> sequences = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(sequencesFName));
            String line;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(" ");

                List<DataContainer> sequence = new ArrayList<>();
                for(String item : items) {
                    String[] els = item.split(":");
                    if(els.length == 4) {
                        sequence.add(new DataContainer(els[0], els[1], els[2], els[3]));
                    } else {
                        sequence.add(new DataContainer(els[0], els[1], els[2]));
                    }
                }
                sequences.add(sequence);
            }
            br.close();

        } catch (FileNotFoundException e) {
            log.fatal(e.getMessage());
        } catch (IOException e) {
            log.fatal(e.getMessage());
        }

        loadSequences(sequences);
    }


    /**
     *
     *
     * @param sequencesFName the sequence file to load sequences from
     */
    public void refreshSequences(String sequencesFName) {


        AlgoVMSP algo = new AlgoVMSP();



        this.sequences.clear();
        loadSequences(sequencesFName);
    }


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

    public void createContext(Heuristic iterator) {
        if(iterator instanceof FetchProgressively) {
            PrefetchingContext context = new PrefetchingContext(iterator);
            synchronized (activeContexts) {
                activeContexts.add(context);
            }
        }
    }


    public List<PrefetchingContext> matchContext(DataContainer dc) {
        if(heuristic != FETCH_PROGRESSIVELY) {
            return null;
        }

        List<PrefetchingContext> toRemove = new ArrayList<>();
        List<PrefetchingContext> result = new ArrayList<>();
        synchronized (activeContexts) {
            for (PrefetchingContext context : activeContexts) {
                if (context.matches(dc)) {
                    context.setLastRequestedDc(dc);
                    result.add(context);
                } else {
                    toRemove.add(context);
                }
            }
            activeContexts.removeAll(toRemove);
        }
        return result;
    }
}
