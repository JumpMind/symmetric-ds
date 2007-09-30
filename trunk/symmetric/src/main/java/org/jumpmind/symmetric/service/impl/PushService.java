package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;

public class PushService implements IPushService {
    
    private static final Log logger = LogFactory.getLog(PushService.class);

    private IDataExtractorService extractor;
    
    private IAcknowledgeService ackService;

    private ITransportManager transportManager;

    private INodeService nodeService;

    public void setExtractor(IDataExtractorService extractor) {
        this.extractor = extractor;
    }

    public void pushData() {
        info("Push requested.");

        List<Node> clients = nodeService.findNodesToPushTo();
        if (clients != null) {
            for (Node client : clients) {
                pushToClient(client);
            }
        }
        
        info("Push request completed.");
    }

    class ParameterParser {
        private StringTokenizer tokenizer;

        ParameterParser(String string) {
            tokenizer = new StringTokenizer(string, "&;=");
        }

        BatchInfo nextBatch() {
            if (!tokenizer.hasMoreTokens()) {
                return null;
            }

            String s = tokenizer.nextToken();
            String[] elements = s.split("-");
            assert (elements.length == 2);
            assert (elements[0].equalsIgnoreCase("batch"));
            String batchId = elements[1];
            
            if (!tokenizer.hasMoreTokens()) {
                throw new RuntimeException("Batch ack for batch " + batchId + " doesn't have a status.");
            }
            
            String status = tokenizer.nextToken();
            
            if (status.equalsIgnoreCase(BatchInfo.OK)) {
                return new BatchInfo(batchId);
            } else {
                int line = Integer.parseInt(status);
                return new BatchInfo(batchId, line);
            }
        }
    }

    public void setTransportManager(ITransportManager tm) {
        this.transportManager = tm;
    }

    public void setNodeService(INodeService clientService) {
        this.nodeService = clientService;
    }

    public void setAckService(IAcknowledgeService ackService)
    {
        this.ackService = ackService;
    }
    
    private void pushToClient(Node remote)
    {
        try {
            IOutgoingWithResponseTransport transport = transportManager.getPushTransport(remote, nodeService.findIdentity());

            try {
                
                if (!extractor.extract(remote, transport)) {
                    return;
                }
                 
                BufferedReader reader = transport.readResponse();
                ParameterParser parser = new ParameterParser(reader
                        .readLine());

                List<BatchInfo> batches = new ArrayList<BatchInfo>();
                BatchInfo batchInfo = parser.nextBatch();
                while (batchInfo != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Ack -- Batch: "
                                        + batchInfo.getBatchId()
                                        + " outcome: "
                                        + (batchInfo.isOk() ? "OK" : "error"));
                    }
                    batches.add(batchInfo);
                    batchInfo = parser.nextBatch();
                }
                
                ackService.ack(batches);
            } finally {
                transport.close();
            }
        } catch (ConnectException ex) {
            logger.warn("Server is not available at this url: "
                    + remote.getSyncURL());
        } catch (Exception e) {
            // just report the error because we want to push to other clients
            // in our list
            logger.error(e, e);
        }
    }
    
    private void info(String s)
    {
        if (logger.isInfoEnabled())
        {
            logger.info(s);
        }
    }

}
