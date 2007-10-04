package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;

public interface INodeService {

    public Node findNode(String clientId);
    
    public NodeSecurity findNodeSecurity(String clientId);

    public boolean isNodeAuthorized(String clientId, String password);
   
    public Node findIdentity();

    public List<Node> findNodesToPull();
    
    public List<Node> findNodesToPushTo();
    
    public List<Node> findSourceNodesFor(DataEventAction eventAction);
    
    public List<Node> findTargetNodesFor(DataEventAction eventAction);
    
    public boolean updateNode(Node node);
    
}
