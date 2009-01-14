/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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
package org.jumpmind.symmetric.service.jmx;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

@Deprecated
@ManagedResource(description = "The management interface for SymmetricDS")
public class SymmetricManagementService {

    private IBootstrapService bootstrapService;

    private IParameterService parameterService;

    private IPurgeService purgeService;

    private INodeService nodeService;

    private IDataService dataService;

    private IOutgoingBatchService outgoingBatchService;

    private IRegistrationService registrationService;

    private IDataExtractorService dataExtractorService;

    private IClusterService clusterService;

    private DataSource dataSource;

    @ManagedOperation(description = "Run the purge process")
    public void purge() {
        purgeService.purge();
    }

    @ManagedOperation(description = "Synchronize the triggers")
    public void syncTriggers() {
        bootstrapService.syncTriggers();
    }

    @ManagedAttribute(description = "The properties configured for this symmetric instance")
    public String getPropertiesList() {
        Map<String, String> properties = parameterService.getAllParameters();
        StringBuilder buffer = new StringBuilder();
        for (String key : properties.keySet()) {
            buffer.append(key).append("=").append(properties.get(key)).append("<br/>");
        }
        return buffer.toString();
    }

    @ManagedAttribute(description = "The group this node belongs to")
    public String getNodeGroupId() {
        return parameterService.getNodeGroupId();
    }

    @ManagedAttribute(description = "An external name give to this symmetric node")
    public String getExternalId() {
        return parameterService.getExternalId();
    }

    @ManagedAttribute(description = "Whether the basic data source is being used as the default datasource.")
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

    @Deprecated
    @ManagedOperation(description = "Deprecated. Check to see if the external id is registered")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "externalId", description = "The external id for a node") })
    public boolean isExternalIdRegistered(String externalId) {
        return nodeService.isExternalIdRegistered("store", externalId);
    }

    @ManagedOperation(description = "Check to see if the initial load for a node id is complete.  This method will throw an exception if the load error'd out or was never started.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id") })
    public boolean isInitialLoadComplete(String nodeId) {
        return outgoingBatchService.isInitialLoadComplete(nodeId);
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

    @Deprecated
    @ManagedOperation(description = "Deprecated. Enable or disable a channel for a specific external id. ")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "ignore", description = "Set to true to enable and false to disable"),
            @ManagedOperationParameter(name = "channelId", description = "The channel id to enable or disable"),
            @ManagedOperationParameter(name = "externalId", description = "The external id for a node") })
    public void ignoreNodeChannelForExternalId(boolean ignore, String channelId, String externalId) {
        nodeService.ignoreNodeChannelForExternalId(ignore, channelId, "store", externalId);
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
            @ManagedOperationParameter(name = "tableName", description = "The table name the SQL is for."),
            @ManagedOperationParameter(name = "sql", description = "The SQL statement to send.") })
    public String sendSQL(String nodeId, String tableName, String sql) {
        return dataService.sendSQL(nodeId, tableName, sql);
    }

    @ManagedOperation(description = "Send a delete and reload of a table to a node.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "nodeId", description = "The node id to reload."),
            @ManagedOperationParameter(name = "tableName", description = "The table name to reload.") })
    public String reloadTable(String nodeId, String tableName) {
        return dataService.reloadTable(nodeId, tableName);
    }

    @ManagedOperation(description = "Send a delete and reload of a table to a node.")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeId", description = "The node id to reload."),
            @ManagedOperationParameter(name = "tableName", description = "The table name to reload."),
            @ManagedOperationParameter(name = "overrideInitialLoadSelect", description = "Override initial load select where-clause.") })
    public String reloadTable(String nodeId, String tableName, String overrideInitialLoadSelect) {
        return dataService.reloadTable(nodeId, tableName, overrideInitialLoadSelect);
    }

    @ManagedOperation(description = "Show a batch in Symmetric Data Format.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "batchId", description = "The batch ID to display") })
    public String showBatch(String batchId) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOutgoingTransport transport = new InternalOutgoingTransport(out);
        dataExtractorService.extractBatchRange(transport, batchId, batchId);
        transport.close();
        out.close();
        return out.toString();
    }

    @ManagedOperation(description = "Write a range of batches to a file in Symmetric Data Format.")
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

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
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

}
