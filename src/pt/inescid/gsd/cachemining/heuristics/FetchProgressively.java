package pt.inescid.gsd.cachemining.heuristics;

import pt.inescid.gsd.cachemining.DataContainer;
import pt.inescid.gsd.cachemining.Node;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * Created by sesteves on 22-06-2017.
 */
public class FetchProgressively extends Heuristic {

    private static final int FIRST_STAGE_DEPTH = 3;

    private Node parent;

    private Node currentNode = null;

    private int currentChild = 0;

    private int currentDepth;

    private Queue<Node> queue = new LinkedList<>();

    private boolean block = false;

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
                    parent = queue.poll();
                    currentChild = 0;
                    currentNode = parent.getChildren().get(currentChild);
                    if(currentNode.getChildren() != null) {
                        queue.add(currentNode);

                        // data containers per level
                        containersPerLevel.get(currentDepth - 1).add(currentNode.getValue());
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
                    if(currentDepth == FIRST_STAGE_DEPTH) {
                        block = true;
                    }

                    // data containers per level
                    containersPerLevel.add(new HashSet<DataContainer>());

                } else if (currentNode.getChildren() != null) {
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
}
