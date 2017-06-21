package pt.inescid.gsd.cachemining.heuristics;

import pt.inescid.gsd.cachemining.DataContainer;
import pt.inescid.gsd.cachemining.Node;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by sesteves on 21-06-2017.
 */
public class FetchAll extends Heuristic {

    private static final int MAX_DEPTH = 100;

    private Node parent;

    private Node currentNode = null;

    private int currentChild = 0;

    private int currentDepth;

    private Queue<Node> queue = new LinkedList<>();

    public FetchAll(Node root) {
        parent = root;
        if(parent.getChildren() != null) {
            currentNode = parent.getChildren().get(currentChild);
            queue.add(currentNode);
            currentDepth = 1;
        }
    }

    @Override
    public boolean hasNext() {
        return currentNode != null;
    }

    @Override
    public DataContainer next() {
        Node result = currentNode;

        do {
            if (parent.getChildren().size() - 1 == currentChild) {
                if (!queue.isEmpty()) {
                    parent = queue.poll();
                    currentChild = 0;
                    currentNode = parent.getChildren().get(currentChild);
                    if(currentNode.getChildren() != null) {
                        queue.add(currentNode);
                    }
                } else {
                    currentNode = null;
                    break;
                }
            } else {
                currentNode = parent.getChildren().get(++currentChild);
                if (currentNode.getValue() == null) {
                    currentDepth++;
                } else if (currentNode.getChildren() != null) {
                    queue.add(currentNode);
                }
            }
        } while (currentNode.getValue() == null);

        return result.getValue();
    }

    @Override
    public void remove() {
        if(hasNext()) {
            next();
        }
    }
}
