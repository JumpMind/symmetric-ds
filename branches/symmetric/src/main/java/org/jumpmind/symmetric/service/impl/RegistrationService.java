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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;

public class RegistrationService extends AbstractService implements
        IRegistrationService {

    protected static final Log logger = LogFactory
            .getLog(RegistrationService.class);

    private INodeService nodeService;

    private IDataExtractorService dataExtractorService;
    
    private IAcknowledgeService acknowledgeService;

    private IConfigurationService configurationService;

    private String findClientToRegisterSql;

    private String registerClientSql;

    private String registerClientSecuritySql;

    private String reopenRegistrationSql;

    private String openRegistrationClientSql;

    private String openRegistrationClientSecuritySql;

    /**
     * Register a client for the given domain name and domain ID if the
     * registration is open.
     */
    public boolean registerNode(Node node, OutputStream out) throws IOException {
        String clientId = (String) jdbcTemplate.queryForObject(
                findClientToRegisterSql, new Object[] { node.getNodeGroupId(),
                        node.getExternalId() }, String.class);
        if (clientId == null) {
            return false;
        }
        node.setNodeId(clientId);
        jdbcTemplate.update(registerClientSecuritySql, new Object[] { node
                .getNodeId() });
        jdbcTemplate.update(registerClientSql, new Object[] {
                node.getSyncURL().toString(), node.getSchemaVersion(),
                node.getDatabaseType(), node.getDatabaseVersion(),
                node.getSymmetricVersion(), node.getNodeId() });
        return writeConfiguration(node, out);
    }

    /**
     * Synchronize client configuration.
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
                    // acknowledge right away, because the acknowledgement is not build into the registration
                    // protocol.
                    acknowledgeService.ack(batch.getBatchInfoList());
                }
            }
            dataExtractorService.extractClientIdentityFor(node, transport);
            written = true;
        } else {
            logger
                    .error("There were no configuration tables to return to the client.  There is a good chance that the system is configured incorrectly.");
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
        jdbcTemplate.update(reopenRegistrationSql, new Object[] { password,
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
        String clientId = generateClientId(nodeGroup, externalId);
        String password = generatePassword();
        jdbcTemplate.update(openRegistrationClientSql, new Object[] { clientId,
                nodeGroup, externalId });
        jdbcTemplate.update(openRegistrationClientSecuritySql, new Object[] {
                clientId, password });
    }

    /**
     * Generate a secure random password for a client.
     */
    // TODO: clientGenerator.generatePassword();
    protected String generatePassword() {
        return new RandomDataImpl().nextSecureHexString(30);
    }

    /**
     * Generate the next client ID that is available. Try to use the domain ID
     * as the client ID.
     */
    // TODO: clientGenerator.generateClientId();
    protected String generateClientId(String nodeGroupId, String externalId) {
        String clientId = externalId;
        int maxTries = 100;
        for (int sequence = 0; sequence < maxTries; sequence++) {
            if (nodeService.findNode(clientId) == null) {
                return clientId;
            }
            clientId = externalId + "-" + sequence;
        }
        throw new RuntimeException("Could not find clientId for domainId of "
                + externalId + " after " + maxTries + " tries.");
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setOpenRegistrationClientSecuritySql(
            String openRegistrationClientSecuritySql) {
        this.openRegistrationClientSecuritySql = openRegistrationClientSecuritySql;
    }

    public void setOpenRegistrationClientSql(String openRegistrationClientSql) {
        this.openRegistrationClientSql = openRegistrationClientSql;
    }

    public void setRegisterClientSecuritySql(String registerClientSecuritySql) {
        this.registerClientSecuritySql = registerClientSecuritySql;
    }

    public void setRegisterClientSql(String registerClientSql) {
        this.registerClientSql = registerClientSql;
    }

    public void setReopenRegistrationSql(String reopenRegistrationSql) {
        this.reopenRegistrationSql = reopenRegistrationSql;
    }

    public void setFindClientToRegisterSql(String findClientToRegisterSql) {
        this.findClientToRegisterSql = findClientToRegisterSql;
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

}
