package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;

public interface INodeCommunicationService {

    public List<NodeCommunication> list(CommunicationType communicationType);

    public void save(NodeCommunication nodeCommunication);

    public boolean execute(NodeCommunication nodeCommunication, RemoteNodeStatuses statuses, INodeCommunicationExecutor executor);

    public int getAvailableThreads(CommunicationType communicationType);

    public void stop();

    public interface INodeCommunicationExecutor {
        public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status);
    }
}
