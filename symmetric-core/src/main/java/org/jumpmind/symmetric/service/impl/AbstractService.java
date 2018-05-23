/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IService;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractService implements IService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected IParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;

    protected ISqlTemplate sqlTemplate;
    
    protected ISqlTemplate sqlTemplateDirty;

    protected IDatabasePlatform platform;

    protected String tablePrefix;

    private ISqlMap sqlMap;
    
    private Set<String> logOnce = new HashSet<String>();

    public AbstractService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        this.symmetricDialect = symmetricDialect;
        this.parameterService = parameterService;
        this.tablePrefix = parameterService.getTablePrefix();
        this.platform = symmetricDialect.getPlatform();
        this.sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();
        this.sqlTemplateDirty = symmetricDialect.getPlatform().getSqlTemplateDirty();
    }
    
    protected Date maxDate(Date... dates) {
        Date date = null;
        if (dates != null) {
            for (Date d : dates) {
                if (d != null) {
                    if (date == null || d.after(date)) {
                        date = d;
                    }
                }
            }
        }
        
        return date;
    }

    protected void setSqlMap(ISqlMap sqlMap) {
        this.sqlMap = sqlMap;
    }

    public ISqlTemplate getJdbcTemplate() {
        return symmetricDialect.getPlatform().getSqlTemplate();
    }

    synchronized public void synchronize(Runnable runnable) {
        runnable.run();
    }

    protected boolean isSet(Object value) {
        if (value != null && value.toString().equals("1")) {
            return true;
        } else {
            return false;
        }
    }

    protected Map<String, String> createSqlReplacementTokens() {
        Map<String, String> replacementTokens = createSqlReplacementTokens(this.tablePrefix, symmetricDialect.getPlatform()
                .getDatabaseInfo().getDelimiterToken(), symmetricDialect.getPlatform());
        replacementTokens.putAll(symmetricDialect.getSqlReplacementTokens());
        return replacementTokens;
    }

    public static Map<String, String> createSqlReplacementTokens(String tablePrefix,
            String quotedIdentifier, IDatabasePlatform platform) {
        Map<String, String> map = new HashMap<String, String>();
        List<String> tables = TableConstants.getTablesWithoutPrefix();

            for (String table : tables) {
                map.put(table, String.format("%s%s%s", tablePrefix,
                        StringUtils.isNotBlank(tablePrefix) ? "_" : "", table));
            } 
      
        return map;
    }

    public String getSql(String... keys) {
        if (sqlMap != null) {
            return sqlMap.getSql(keys);
        } else {
            return null;
        }
    }

    public IParameterService getParameterService() {
        return parameterService;
    }

    public ISymmetricDialect getSymmetricDialect() {
        return symmetricDialect;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    protected void close(ISqlTransaction transaction) {
        if (transaction != null) {
            transaction.close();
        }
    }
    
    protected Set<String> toNodeIds(Set<Node> nodes) {
        return toNodeIds(nodes, null);
    }
    
    protected Set<String> toNodeIds(Set<Node> nodes, Set<String> nodeIds) {
        nodeIds = nodeIds == null ? new HashSet<String>(nodes.size()) : nodeIds;
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }

    protected boolean isCalledFromSymmetricAdminTool() {
        boolean adminTool = false;
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : trace) {
            adminTool |= stackTraceElement.getClassName().equals(
                    "org.jumpmind.symmetric.SymmetricAdmin");
        }
        return adminTool;
    }
    
    protected String buildBatchWhere(List<String> nodeIds, List<String> channels,
            List<?> statuses, List<Long> loads) {
        boolean containsErrorStatus = statuses.contains(OutgoingBatch.Status.ER)
                || statuses.contains(IncomingBatch.Status.ER);
        boolean containsIgnoreStatus = statuses.contains(OutgoingBatch.Status.IG)
                || statuses.contains(IncomingBatch.Status.IG);

        StringBuilder where = new StringBuilder();
        boolean needsAnd = false;
        if (nodeIds != null && nodeIds.size() > 0) {
            where.append("node_id in (:NODES)");
            needsAnd = true;
        }
        
        if (channels != null && channels.size() > 0) {
            if (needsAnd) {
                where.append(" and ");
            }
            where.append("channel_id in (:CHANNELS)");
            needsAnd = true;
        }
        if (loads != null && loads.size() > 0) {
            if (needsAnd) {
                where.append(" and ");
            }
            where.append("load_id in (:LOADS)");
            needsAnd = true;
        }
        if (statuses != null && statuses.size() > 0) {
            if (needsAnd) {
                where.append(" and ");
            }
            where.append("(status in (:STATUSES)");
            
            if (containsErrorStatus) {
                where.append(" or error_flag = 1 ");   
            }
            
            if (containsIgnoreStatus) {
                where.append(" or ignore_count > 0 ");   
            }
            
            where.append(")");

            needsAnd = true;
        }
        
        if (where.length() > 0) {
            where.insert(0, " where ");
        }
        return where.toString();
    }
    
    /**
     * Try a configured number of times to get the ACK through.
     */
    protected void sendAck(Node remote, Node local, NodeSecurity localSecurity,
            List<IncomingBatch> list, ITransportManager transportManager) throws IOException {
        assertNotNull(remote, "Node remote cannot be null. Maybe there is a missing sym_node row.");
        assertNotNull(local, "Node local cannot be null. Maybe there is a missing sym_node row.");
        assertNotNull(localSecurity, "NodeSecurity localSecurity cannot be null. Maybe there is a missing sym_node_security row.");
        
        Exception exception = null;
        int statusCode = -1;
        int numberOfStatusSendRetries = parameterService
                .getInt(ParameterConstants.DATA_LOADER_NUM_OF_ACK_RETRIES);
        for (int i = 0; i < numberOfStatusSendRetries && statusCode != HttpURLConnection.HTTP_OK; i++) {
            try {
                statusCode = transportManager.sendAcknowledgement(remote, list, local,
                        localSecurity.getNodePassword(), parameterService.getRegistrationUrl());
            } catch (IOException ex) {
                exception = ex;
            } catch (RuntimeException ex) {
                exception = ex;
            }
            if (statusCode != HttpURLConnection.HTTP_OK) {
                String message = String.format("Ack was not sent successfully on try number %s of %s.", i+1, numberOfStatusSendRetries);
                if (statusCode > 0) {
                    message += String.format(" statusCode=%s", statusCode);
                }
                if (exception != null) {
                    log.warn(message, exception);
                } else {
                    log.warn(message);   
                }
                
                if (i < numberOfStatusSendRetries - 1) {
                    AppUtils.sleep(parameterService
                            .getLong(ParameterConstants.DATA_LOADER_TIME_BETWEEN_ACK_RETRIES));
                } else if (exception instanceof RuntimeException) {
                    throw (RuntimeException) exception;
                } else if (exception instanceof IOException) {
                    throw (IOException) exception;
                } else {
                    throw new IOException("Ack was not sent successfully. statusCode=" + statusCode);
                }
            }
        }
    }    
    
    protected List<BatchAck> readAcks(List<OutgoingBatch> batches, IOutgoingWithResponseTransport transport,
            ITransportManager transportManager, IAcknowledgeService acknowledgeService, IDataExtractorService dataExtratorService)
            throws IOException {

        Set<Long> batchIds = new HashSet<Long>(batches.size());
        for (OutgoingBatch outgoingBatch : batches) {
            if (outgoingBatch.getStatus() == OutgoingBatch.Status.LD) {
                batchIds.add(outgoingBatch.getBatchId());
            }
        }

        BufferedReader reader = transport.readResponse();
        String ackString = reader.readLine();
        String ackExtendedString = reader.readLine();

        log.debug("Reading ack: {}", ackString);
        log.debug("Reading extend ack: {}", ackExtendedString);

        String line = null;
        do {
            line = reader.readLine();
            if (line != null) {
                log.info("Read another unexpected line {}", line);
            }
        } while (line != null);

        List<BatchAck> batchAcks = transportManager.readAcknowledgement(ackString,
                ackExtendedString);

        long batchIdInError = Long.MAX_VALUE;
        for (BatchAck batchInfo : batchAcks) {
            batchIds.remove(batchInfo.getBatchId());
            if (!batchInfo.isOk()) {
                batchIdInError = batchInfo.getBatchId();
            }
            log.debug("Saving ack: {}, {}", batchInfo.getBatchId(),
                    (batchInfo.isOk() ? "OK" : "ER"));
            acknowledgeService.ack(batchInfo);
        }

        for (Long batchId : batchIds) {
            if (batchId < batchIdInError) {                
                for (OutgoingBatch outgoingBatch : batches) {
                    if (outgoingBatch.getBatchId() == batchId) {
                        StringBuilder message = new StringBuilder(128);
                        message.append("Expected but did not receive an ack for batch ");
                        message.append(outgoingBatch.getNodeBatchId()).append(". ");
                        
                        if (dataExtratorService != null) {
                            if (!outgoingBatch.isLoadFlag()) {
                                message.append("This could be because the batch is corrupt - removing the batch from staging.");
                                log.warn(message.toString());
                                dataExtratorService.removeBatchFromStaging(outgoingBatch);
                            } else {
                                message.append("This could be because the batch is corrupt. Not removing the batch because it was a load batch, but you may need to clear the batch from staging manually.");
                                log.warn(message.toString());
                            }
                        } else {
                            log.warn(message.toString());
                        }
                    }
                }
            }
        }

        return batchAcks;
    }
    
    protected void logOnce(String message) {
        if (!logOnce.contains(message)) {
            logOnce.add(message);
            log.info(message);
        }
    }
    
    protected boolean isStreamClosedByClient(Exception ex) {
        if (ExceptionUtils.indexOfType(ex, EOFException.class) >= 0) {
            return true;
        } else {
            return false;
        }
    }
    
    protected void assertNotNull(Object o, String message) {
        if (o == null) {
            throw new SymmetricException(message);
        }
    }

    

}