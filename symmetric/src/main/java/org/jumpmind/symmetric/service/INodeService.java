package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;

public interface INodeService {

    public Node findNode(String clientId);
    
    public NodeSecurity findNodeSecurity(String clientId);

    public boolean isNodeAuthorized(String clientId, String password);
   
    public Node findIdentity();

    public List<Node> findNodesToPull();
    
    public List<Node> findNodesToPushTo();
    
}
