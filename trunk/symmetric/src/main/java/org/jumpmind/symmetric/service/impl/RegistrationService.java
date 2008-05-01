/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Types;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;

// TODO: NodeService already does all this DML.  Should use NodeService or move methods to there.
public class RegistrationService extends AbstractService implements
        IRegistrationService {

    protected static final Log logger = LogFactory
            .getLog(RegistrationService.class);

    private INodeService nodeService;

    private IDataExtractorService dataExtractorService;
    
    private IAcknowledgeService acknowledgeService;

    private IConfigurationService configurationService;
    
    private IClusterService clusterService;

    private IDataService dataService;
    
    /**
     * Register a node for the given domain name and domain ID if the
     * registration is open.
     */
    public boolean registerNode(Node node, OutputStream out) throws IOException {
        if (! configurationService.isRegistrationServer()) {
            // registration is not allowed until this node has an initial load
            NodeSecurity security = nodeService.findNodeSecurity(nodeService.findIdentity().getNodeId());
            if (security != null && security.getInitialLoadTime() == null) {
                return false;
            }
        }
        String nodeId = findNodeToRegister(node.getNodeGroupId(), node.getExternalId());
        if (nodeId == null && parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED)) {
            openRegistration(node.getNodeGroupId(), node.getExternalId());
            nodeId = findNodeToRegister(node.getNodeGroupId(), node.getExternalId());
        }
        if (nodeId == null) {
            return false;
        }
        node.setNodeId(nodeId);
        jdbcTemplate.update(getSql("registerNodeSecuritySql"), new Object[] { node
                .getNodeId() });
        jdbcTemplate.update(getSql("registerNodeSql"), new Object[] { node.getSyncURL().toString(),
                node.getSchemaVersion(), node.getDatabaseType(), node.getDatabaseVersion(),
                node.getSymmetricVersion(), node.getNodeId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        boolean success = writeConfiguration(node, out);
        if (success && parameterService.is(ParameterConstants.AUTO_RELOAD_ENABLED)) {
            // only send automatic initial load once
            NodeSecurity security = nodeService.findNodeSecurity(node.getNodeId());
            if (security != null && security.getInitialLoadTime() == null) {
                dataService.reloadNode(node.getNodeId());
            }
        }
        return success;
    }

    private String findNodeToRegister(String nodeGroupId, String externald) {
        return (String) jdbcTemplate.queryForObject(getSql("findNodeToRegisterSql"), new Object[] { nodeGroupId,
                externald }, String.class);
    }

    /**
     * Synchronize node configuration.
     */
    protected boolean writeConfiguration(Node node, OutputStream out)
            throws IOException {
        boolean written = false;
        IOutgoingTransport transport = new InternalOutgoingTransport(out);
        List<String> tableNames = configurationService
                .getRootConfigChannelTableNames();
        if (tableNames != null && tableNames.size() > 0) {
            for (String tableName : tableNames) {
                Trigger trigger = configurationService
                        .getTriggerForTarget(tableName, runtimeConfiguration
                                .getNodeGroupId(), node.getNodeGroupId(),
                                Constants.CHANNEL_CONFIG);
                if (trigger != null) {
                    OutgoingBatch batch = dataExtractorService.extractInitialLoadFor(node,
                            trigger, transport);
                    // acknowledge right away, because the acknowledgment is not build into the registration
                    // protocol.
                    acknowledgeService.ack(batch.getBatchInfo());
                }
            }
            dataExtractorService.extractNodeIdentityFor(node, transport);
            written = true;
        } else {
            logger
                    .error("There were no configuration tables to return to the node.  There is a good chance that the system is configured incorrectly.");
        }
        transport.close();
        return written;
    }

    /**
     * Re-open registration for a single node that already exists in the
     * database. A new password is generated and the registration_enabled flag
     * is turned on. The next node to try registering for this node group and
     * external ID will be given this information.
     */
    public void reOpenRegistration(String nodeId) {
        String password = generatePassword();
        jdbcTemplate.update(getSql("reopenRegistrationSql"), new Object[] { password,
                nodeId });
    }

    /**
     * Open registration for a single new node given a node group (f.e., "STORE")
     * and external ID (f.e., "00001"). The unique node ID and password are
     * generated and stored in the node and node_security tables with the
     * registration_enabled flag turned on. The next node to try registering
     * for this node group and external ID will be given this information.
     */
    public void openRegistration(String nodeGroup, String externalId) {
        String nodeId = generateNodeId(nodeGroup, externalId);
        String password = generatePassword();
        jdbcTemplate.update(getSql("openRegistrationNodeSql"), new Object[] { nodeId,
                nodeGroup, externalId });
        jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] {
                nodeId, password });        
        clusterService.initLockTableForNode(nodeService.findNode(nodeId));
    }

    /**
     * Generate a secure random password for a node.
     */
    // TODO: nodeGenerator.generatePassword();
    protected String generatePassword() {
        return new RandomDataImpl().nextSecureHexString(30);
    }

    /**
     * Generate the next node ID that is available. Try to use the domain ID
     * as the node ID.
     */
    // TODO: nodeGenerator.generateNodeId();
    protected String generateNodeId(String nodeGroupId, String externalId) {
        String nodeId = externalId;
        int maxTries = 100;
        for (int sequence = 0; sequence < maxTries; sequence++) {
            if (nodeService.findNode(nodeId) == null) {
                return nodeId;
            }
            nodeId = externalId + "-" + sequence;
        }
        throw new RuntimeException("Could not find nodeId for externalId of "
                + externalId + " after " + maxTries + " tries.");
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDataExtractorService(
            IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setConfigurationService(
            IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setAcknowledgeService(IAcknowledgeService acknowledgeService) {
        this.acknowledgeService = acknowledgeService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public boolean isAutoRegistration() {
        return parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED);
    }

}
