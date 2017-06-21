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

    public DataContainer getValue() {
        return value;
    }

    public void setValue(DataContainer value) {
        this.value = value;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }
}
