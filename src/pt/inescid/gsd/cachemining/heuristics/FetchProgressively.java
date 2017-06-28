package pt.inescid.gsd.cachemining.heuristics;

import pt.inescid.gsd.cachemining.DataContainer;
import pt.inescid.gsd.cachemining.Node;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Created by sesteves on 22-06-2017.
 */
public class FetchProgressively extends Heuristic {

    private static final int FIRST_STAGE_DEPTH = 2;

    private Node parent;

    private Node currentNode = null;

    private int currentChild = 0;

    private int currentDepth;

    private Queue<Node> queue = new LinkedList<>();

    private boolean block = false;

    private Map<DataContainer, Set<DataContainer>> pairPath = new HashMap<>();

    private Set<DataContainer> previousSet;

    private Set<DataContainer> set = new HashSet<>();

    public FetchProgressively(Node root) {
        parent = root;
        if(parent.getChildren() != null) {
            currentNode = parent.getChildren().get(currentChild);
            queue.add(currentNode);
            currentDepth = 1;

            // data containers per level
            Set<DataContainer> set = new HashSet<>();
            set.add(currentNode.getValue());
            containersPerLevel.add(set);
        }
    }

    @Override
    public boolean hasNext() {
        return !block && currentNode != null;
    }

    @Override
    public DataContainer next() {
        Node result = currentNode;

        do {
            // if there are no more children to explore
            if (parent.getChildren().size() - 1 == currentChild) {
                if (!queue.isEmpty()) {

                    if (previousSet != null && previousSet.isEmpty()) {
                        currentNode = null;
                        currentDepth++;
                        break;
                    }
                    do {
                        parent = queue.poll();
                    } while (parent != null && previousSet != null && !previousSet.remove(parent.getValue()));
                    if(parent == null) {
                        currentNode = null;
                        currentDepth++;
                        break;
                    }


                    set = new HashSet<>();
                    pairPath.put(parent.getValue(), set);


                    currentChild = 0;
                    currentNode = parent.getChildren().get(currentChild);


                    // data containers per level
                    containersPerLevel.get(currentDepth - 1).add(currentNode.getValue());

                    if(currentNode.getChildren() != null) {
                        queue.add(currentNode);
                        set.add(currentNode.getValue());
                    }

                } else {
                    currentNode = null;
                    break;
                }
            } else {
                currentNode = parent.getChildren().get(++currentChild);
                if (currentNode.getValue() == null) {
                    // go to the next level
                    currentDepth++;

                    // block iterator if we reach first stage depth
                    if (currentDepth > FIRST_STAGE_DEPTH) {
                        block = true;
                        break;
                    }

                    // data containers per level
                    containersPerLevel.add(new HashSet<DataContainer>());

                } else if (currentNode.getChildren() != null) {
                    set.add(currentNode.getValue());
                    queue.add(currentNode);

                    // data containers per level
                    containersPerLevel.get(currentDepth - 1).add(currentNode.getValue());
                }
            }
        } while (currentNode.getValue() == null);

        return result.getValue();
    }

    @Override
    public void remove() {

    }

    public boolean unblock(DataContainer dc) {
        // wait until iterator is no longer in use
        while(hasNext());

        previousSet = pairPath.get(dc);
        if (previousSet == null) {
           return false;
        }
        pairPath.clear();
        do {
            parent = queue.poll();
        } while (parent != null && !previousSet.remove(parent.getValue()));
        if (parent == null) {
            return false;
        }
        block = false;

        currentChild = 0;
        currentNode = parent.getChildren().get(currentChild);

        set = new HashSet<>();
        pairPath.put(parent.getValue(), set);

        if(currentNode.getChildren() != null) {
            queue.add(currentNode);
            set.add(currentNode.getValue());
        }

        Set<DataContainer> s = new HashSet<>();
        s.add(currentNode.getValue());
        containersPerLevel.add(s);

        return true;
    }

    public boolean hasNextStage(DataContainer dc) {
        return pairPath.containsKey(dc) && pairPath.get(dc).size() > 0;
    }
}
