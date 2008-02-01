/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
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
        List<Node> nodes = nodeService.findNodesToPushTo();
        if (nodes != null && nodes.size() > 0) {
            info("Push requested");
            for (Node node : nodes) {
                pushToNode(node);
            }
            info("Push request completed");
        }        
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
                throw new RuntimeException("Batch ack for batch " + batchId
                        + " doesn't have a status.");
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

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setAckService(IAcknowledgeService ackService) {
        this.ackService = ackService;
    }

    private void pushToNode(Node remote) {
        try {
            IOutgoingWithResponseTransport transport = transportManager
                    .getPushTransport(remote, nodeService.findIdentity());

            try {

                if (!extractor.extract(remote, transport)) {
                    return;
                }

                debug("Just pushed data, about to read the response.");

                BufferedReader reader = transport.readResponse();
                ParameterParser parser = new ParameterParser(reader.readLine());

                List<BatchInfo> batches = new ArrayList<BatchInfo>();
                BatchInfo batchInfo = parser.nextBatch();
                while (batchInfo != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Ack -- Batch: " + batchInfo.getBatchId()
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
            logger.warn(ErrorConstants.COULD_NOT_CONNECT_TO_TRANSPORT + " url=" + remote.getSyncURL());
        } catch (ConnectionRejectedException ex) {
            logger.warn(ErrorConstants.TRANSPORT_REJECTED_CONNECTION);
        } catch (SocketException ex) {
            logger.warn(ex.getMessage());
        } catch (AuthenticationException ex) {
            logger.warn(ErrorConstants.NOT_AUTHENTICATED);                 
        } catch (Exception e) {
            // just report the error because we want to push to other nodes
            // in our list
            logger.error(e, e);
        }
    }

    private void info(String... s) {
        if (logger.isInfoEnabled()) {
            StringBuilder msg = new StringBuilder();
            for (String string : s) {
                msg.append(string);
                msg.append(" ");
            }
            logger.info(msg);
        }
    }

    private void debug(String... s) {
        if (logger.isDebugEnabled()) {
            StringBuilder msg = new StringBuilder();
            for (String string : s) {
                msg.append(string);
                msg.append(" ");
            }
            logger.debug(msg);
        }
    }

}
