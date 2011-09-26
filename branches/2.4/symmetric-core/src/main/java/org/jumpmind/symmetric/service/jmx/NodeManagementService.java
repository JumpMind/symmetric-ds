/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.service.jmx;

import java.io.FileOutputStream;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SecurityConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager.NodeConnectionStatistics;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for a node")
/**
 * ,
 */
public class NodeManagementService {
    
    final ILog log = LogFactory.getLog(getClass());

    private IPurgeService purgeService;

    private INodeService nodeService;

    private IDataService dataService;

    private IOutgoingBatchService outgoingBatchService;
    
    private IConfigurationService configurationService;
    
    private ITriggerRouterService triggerRouterService;

    private IRegistrationService registrationService;

    private IDataExtractorService dataExtractorService;

    private IClusterService clusterService;

    private IParameterService parameterService;

    private IConcurrentConnectionManager concurrentConnectionManager;

    private ISecurityService securityService;

    private DataSource dataSource;

    IStatisticManager statisticManager;

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    @ManagedAttribute(description = "Checks if SymmetricDS has been started.")
    public boolean isStarted() {
        ISymmetricEngine engine = getEngine();
        if (engine != null) {
            return engine.isStarted();
        } else {
            return false;
        }
    }
    
    @ManagedOperation(description = "Start the SymmetricDS engine")
    public boolean start() {
        try {
            ISymmetricEngine engine = getEngine();
            if (engine != null) {
                return engine.start();
            } else {
                return false;
            }
        } catch (Exception ex) {
            log.error(ex);
            return false;
        }
    }

    @ManagedOperation(description = "Stop the SymmetricDS engine")
    public void stop() {
        try {
            ISymmetricEngine engine = getEngine();
            if (engine != null) {
                engine.stop();
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }
    
    @ManagedOperation(description = "Run the purge process")
    public void purge() {
        purgeService.purgeOutgoing();
    }        
    
    @ManagedOperation(description = "Force the channel settings to be read from the database")
    public void clearChannelCache() {
        configurationService.reloadChannels();
    }

    @ManagedOperation(description = "Synchronize the triggers")
    public void syncTriggers() {
        triggerRouterService.syncTriggers();
    }
    
    protected ISymmetricEngine getEngine() {
        return AbstractSymmetricEngine.findEngineByName(parameterService.getString(ParameterConstants.ENGINE_NAME));
    }
    
    @ManagedAttribute(description = "Get the number of current connections allowed to this "
            + "instance of the node via HTTP.  If this value is 20, then 20 concurrent push"
            + " clients and 20 concurrent pull clients will be allowed")
    public int getNumfNodeConnectionsPerInstance() {
        return parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS);
    }

    @ManagedAttribute(description = "Get connection statistics about indivdual nodes")
    public String getNodeConcurrencyStatisticsAsText() {
        String lineFeed = "\n";
        if (parameterService.getString(ParameterConstants.JMX_LINE_FEED).equals("html")) {
            lineFeed = "</br>";
        }
        Map<String, Map<String, NodeConnectionStatistics>> stats = concurrentConnectionManager
                .getNodeConnectionStatisticsByPoolByNodeId();
        StringBuilder out = new StringBuilder();
        for (String pool : stats.keySet()) {
            out
            .append("-------------------------------------------------------------------------------------------------------------------------------");            out.append(lineFeed);
            out.append("  CONNECTION TYPE: ");
            out.append(pool);
            out.append(lineFeed);
            out
                    .append("-------------------------------------------------------------------------------------------------------------------------------");
            out.append(lineFeed);
            out
                    .append("             NODE ID             LAST CONNECT TIME      NUMBER OF CONNECTIONS     NUMBER OF REJECTIONS       AVG CONNECTED TIME");
            out.append(lineFeed);
            out
            .append("-------------------------------------------------------------------------------------------------------------------------------");            out.append(lineFeed);
            Map<String, NodeConnectionStatistics> nodeStats = stats.get(pool);
            for (String nodeId : nodeStats.keySet()) {
                NodeConnectionStatistics nodeStat = nodeStats.get(nodeId);
                out.append(StringUtils.leftPad(nodeId, 20));
                out.append(StringUtils.leftPad(DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(
                        new Date(nodeStat.getLastConnectionTimeMs())), 30));
                out.append(StringUtils.leftPad(Long.toString(nodeStat.getTotalConnectionCount()), 27));
                out.append(StringUtils.leftPad(Integer.toString(nodeStat.getNumOfRejections()), 25));
                out.append(StringUtils.leftPad(NumberFormat.getIntegerInstance().format(
                        nodeStat.getTotalConnectionTimeMs() / nodeStat.getTotalConnectionCount()), 25));
            }
            out.append(lineFeed);
        }
        return out.toString();
    }

    public String getCurrentNodeConcurrencyReservationsAsText() {
        throw new NotImplementedException();
    }

    @ManagedAttribute(description = "Get a list of nodes that have been added to the white list, a list of node ids that always get through the concurrency manager.")
    public String getNodesInWhiteList() {
        StringBuilder ret = new StringBuilder();
        String[] list = concurrentConnectionManager.getWhiteList();
        for (String string : list) {
            ret.append(string);
            ret.append(",");
        }
        return ret.length() > 0 ? ret.substring(0, ret.length() - 1) : "";
    }

    @ManagedOperation(description = "Add a node id to the list of nodes that will always get through the concurrency manager")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id to add to the white list") })
    public void addNodeToWhiteList(String nodeId) {
        concurrentConnectionManager.addToWhitelist(nodeId);
    }

    @ManagedOperation(description = "Remove a node id to the list of nodes that will always get through the concurrency manager")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id to remove from the white list") })
    public void removeNodeFromWhiteList(String nodeId) {
        concurrentConnectionManager.removeFromWhiteList(nodeId);
    }

    @ManagedAttribute(description = "Configure the number of connections allowed to this node."
            + "  If the value is set to zero you are effectively disabling your transport"
            + " (wihch can be useful for maintainance")
    public void setNumOfNodeConnectionsPerInstance(int value) {
        parameterService.saveParameter(ParameterConstants.CONCURRENT_WORKERS, value);
    }

    @ManagedAttribute(description = "The group this node belongs to")
    public String getNodeGroupId() {
        return parameterService.getNodeGroupId();
    }

    @ManagedAttribute(description = "An external name given to this SymmetricDS node")
    public String getExternalId() {
        return parameterService.getExternalId();
    }

    @ManagedAttribute(description = "The node id given to this SymmetricDS node")
    public String getNodeId() {
        Node node = nodeService.findIdentity();
        if (node != null) {
            return node.getNodeId();
        } else {
            return "?";
        }
    }

    @ManagedAttribute(description = "Whether the basic DataSource is being used as the default datasource.")
    public boolean isBasicDataSource() {
        return dataSource instanceof BasicDataSource;
    }

    @ManagedAttribute(description = "If a BasicDataSource, then show the number of active connections")
    public int getNumberOfActiveConnections() {
        if (isBasicDataSource()) {
            return ((BasicDataSource) dataSource).getNumActive();
        } else {
            return -1;
        }
    }

    @ManagedOperation(description = "Check to see if the external id is registered")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeGroupId", description = "The node group id for a node"),
            @ManagedOperationParameter(name = "externalId", description = "The external id for a node") })
    public boolean isExternalIdRegistered(String nodeGroupdId, String externalId) {
        return nodeService.isExternalIdRegistered(nodeGroupdId, externalId);
    }

    @ManagedOperation(description = "Emergency remove all locks (if left abandoned on a cluster)")
    public void clearAllLocks() {
        clusterService.clearAllLocks();
    }

    @ManagedOperation(description = "Check to see if the initial load for a node id is complete.  This method will throw an exception if the load error'd out or was never started.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id") })
    public boolean areAllLoadBatchesComplete(String nodeId) {
        return outgoingBatchService.areAllLoadBatchesComplete(nodeId);
    }

    @ManagedOperation(description = "Enable or disable synchronization completely for a node")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeId", description = "The node to enable or disable"),
            @ManagedOperationParameter(name = "syncEnabled", description = "true is enabled, false is disabled") })
    public boolean setSyncEnabledForNode(String nodeId, boolean syncEnabled) {
        Node node = nodeService.findNode(nodeId);
        if (node != null) {
            node.setSyncEnabled(syncEnabled);
            nodeService.updateNode(node);
            return true;
        } else {
            return false;
        }
    }

    @ManagedOperation(description = "Enable or disable a channel for a specific external id")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "ignore", description = "Set to true to enable and false to disable"),
            @ManagedOperationParameter(name = "channelId", description = "The channel id to enable or disable"),
            @ManagedOperationParameter(name = "nodeGroupId", description = "The node group id for a node"),
            @ManagedOperationParameter(name = "externalId", description = "The external id for a node") })
    public void ignoreNodeChannelForExternalId(boolean ignore, String channelId, String nodeGroupId, String externalId) {
        nodeService.ignoreNodeChannelForExternalId(ignore, channelId, nodeGroupId, externalId);
    }

    @ManagedOperation(description = "Open the registration for a node with the specified external id")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeGroup", description = "The node group id this node will belong to"),
            @ManagedOperationParameter(name = "externalId", description = "The external id for the node") })
    public void openRegistration(String nodeGroupId, String externalId) {
        Node node = nodeService.findNodeByExternalId(nodeGroupId, externalId);
        if (node != null) {
            registrationService.reOpenRegistration(node.getExternalId());
        } else {
            registrationService.openRegistration(nodeGroupId, externalId);
        }
    }

    @ManagedOperation(description = "Send an initial load of data to a node.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id to reload.") })
    public String reloadNode(String nodeId) {
        return dataService.reloadNode(nodeId);
    }

    @ManagedOperation(description = "Send a SQL event to a node.")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeId", description = "The node id to sent the event to."),
            @ManagedOperationParameter(name = "catalogName", description = "The catalog name to reload. Can be null."),
            @ManagedOperationParameter(name = "schemaName", description = "The schema name to reload. Can be null."),                        
            @ManagedOperationParameter(name = "tableName", description = "The table name the SQL is for."),
            @ManagedOperationParameter(name = "sql", description = "The SQL statement to send.") })
    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName, String sql) {
        return dataService.sendSQL(nodeId, catalogName, schemaName, tableName, sql, false);
    }

    @ManagedOperation(description = "Send a delete and reload of a table to a node.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id to reload."),
        @ManagedOperationParameter(name = "catalogName", description = "The catalog name to reload. Can be null."),
        @ManagedOperationParameter(name = "schemaName", description = "The schema name to reload. Can be null."),                    
            @ManagedOperationParameter(name = "tableName", description = "The table name to reload.") })
    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName) {
        return dataService.reloadTable(nodeId, catalogName, schemaName, tableName);
    }

    @ManagedOperation(description = "Send a delete and reload of a table to a node.")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeId", description = "The node id to reload."),
            @ManagedOperationParameter(name = "catalogName", description = "The catalog name to reload. Can be null."),
            @ManagedOperationParameter(name = "schemaName", description = "The schema name to reload. Can be null."),            
            @ManagedOperationParameter(name = "tableName", description = "The table name to reload."),
            @ManagedOperationParameter(name = "overrideInitialLoadSelect", description = "Override initial load select where-clause.") })
    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName, String overrideInitialLoadSelect) {
        return dataService.reloadTable(nodeId, catalogName, schemaName, tableName, overrideInitialLoadSelect);
    }

    @ManagedOperation(description = "Write a range of batches to a file in SymmetricDS Data Format.")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "startBatchId", description = "Starting batch ID of range"),
            @ManagedOperationParameter(name = "endBatchId", description = "Ending batch ID of range"),
            @ManagedOperationParameter(name = "fileName", description = "File name to write batches") })
    public void writeBatchRangeToFile(String startBatchId, String endBatchId, String fileName) throws Exception {
        FileOutputStream out = new FileOutputStream(fileName);
        IOutgoingTransport transport = new InternalOutgoingTransport(out);
        dataExtractorService.extractBatchRange(transport, startBatchId, endBatchId);
        transport.close();
        out.close();
    }

    @ManagedOperation(description = "Encrypts plain text for use with db.user and db.password properties")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "plainText", description = "Plain text to encrypt") })
    public String encryptText(String plainText) throws Exception {
        try {
        return SecurityConstants.PREFIX_ENC + securityService.encrypt(plainText);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return "";
    }
    public void setPurgeService(IPurgeService purgeService) {
        this.purgeService = purgeService;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setConcurrentConnectionManager(IConcurrentConnectionManager concurrentConnectionManager) {
        this.concurrentConnectionManager = concurrentConnectionManager;
    }

    public void setSecurityService(ISecurityService securityService) {
        this.securityService = securityService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }
    
    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }
}