package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IPullService;

public class PullService implements IPullService {
    
    private static final Log logger = LogFactory.getLog(PullService.class);

    private INodeService nodeService;

    private IDataLoaderService dataLoaderService;

    public void pullData() {
        logger.info("Pull requested");
        List<Node> nodes = nodeService.findNodesToPull();
        for (Node node : nodes) {
            try {
                dataLoaderService.loadData(node, nodeService.findIdentity());
            } catch (ConnectException ex) {
                logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT);
            } catch (IOException e) {
                logger.error(e, e);
            }
        }
        logger.info("Pull completed");
    }

    public void setNodeService(INodeService clientService) {
        this.nodeService = clientService;
    }

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }
}
