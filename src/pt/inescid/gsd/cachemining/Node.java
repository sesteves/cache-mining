package pt.inescid.gsd.cachemining;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sesteves on 21-06-2017.
 */
public class Node {

    private DataContainer value = null;

    private List<Node> children;

    public Node() {
    }

    public Node(DataContainer value) {
        this.value = value;
    }

    public void addChild(Node node, double probability) {
        if (children == null) {
            children = new ArrayList<>();
        }
        children.add(node);
    }

    public Node getChild(DataContainer value) {
        if(children != null) {
            for (Node child : children) {
                if (value.equals(child.value)) {
                    return child;
                }
            }
        }
        return null;
    }

    public DataContainer getValue() {
        return value;
    }

    public List<Node> getChildren() {
        return children;
    }
}
