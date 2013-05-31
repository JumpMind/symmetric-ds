package org.jumpmind.symmetric.web.rest.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "nodelist")
public class NodeList {

    List<Node> nodes;

    public NodeList(Node... nodes) {
        this.setNodes(nodes);
    }

    public NodeList() {
        this.nodes = new ArrayList<Node>();
    }

    public void setNodes(Node[] nodes) {
        this.nodes = new ArrayList<Node>();
        for (Node node : nodes) {
            this.nodes.add(node);
        }
    }

    public Node[] getNodes() {
        return nodes.toArray(new Node[nodes.size()]);
    }
    
    public void addNode(Node node) {
        this.nodes.add(node);
    }
}
