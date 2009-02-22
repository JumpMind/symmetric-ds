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
import java.net.ConnectException;
import java.sql.Types;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.RegistrationFailedException;
import org.jumpmind.symmetric.service.RegistrationRedirectException;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.upgrade.UpgradeConstants;
import org.jumpmind.symmetric.util.RandomTimeSlot;
import org.springframework.transaction.annotation.Transactional;

// TODO: NodeService already does all this DML. Should we use the NodeService or move
// methods to there?
public class RegistrationService extends AbstractService implements IRegistrationService {

    protected static final Log logger = LogFactory.getLog(RegistrationService.class);

    private INodeService nodeService;

    private IDataExtractorService dataExtractorService;

    private IConfigurationService configurationService;

    private IClusterService clusterService;

    private IDataService dataService;

    private IDataLoaderService dataLoaderService;

    private ITransportManager transportManager;
    
    private RandomTimeSlot randomTimeSlot;

    private IDbDialect dbDialect;

    /**
     * Register a node for the given group name and external id if the
     * registration is open.
     * @param isRequestedRegistration An indicator that registration has been requested by the remote client
     */
    public boolean registerNode(Node node, OutputStream out, boolean isRequestedRegistration) throws IOException {
        if (!configurationService.isRegistrationServer()) {
            // registration is not allowed until this node has an identity and an initial load
            Node identity = nodeService.findIdentity();
            NodeSecurity security = identity == null ? null : nodeService.findNodeSecurity(identity.getNodeId());
            if (security == null || security.getInitialLoadTime() == null) {
                logger.warn("Registration is not allowed until this node has an initial load");
                return false;
            }
        }
        
        String redirectUrl = getRedirectionUrlFor(node.getExternalId());
        if (redirectUrl != null) {
            logger.info(String.format("Redirecting %s to %s for registration.", node.getExternalId(), redirectUrl));
            throw new RegistrationRedirectException(redirectUrl);
        }
        
        String nodeId = findNodeToRegister(node.getNodeGroupId(), node.getExternalId());
        if (nodeId == null && parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED)) {
            Node existingNode = nodeService.findNodeByExternalId(node.getNodeGroupId(), node.getExternalId());
            if (existingNode != null) {
                nodeId = existingNode.getNodeId();
            } else {
                openRegistration(node.getNodeGroupId(), node.getExternalId());
                nodeId = findNodeToRegister(node.getNodeGroupId(), node.getExternalId());
            }
        }
        
        if (nodeId == null) {
            return false;
        }        
        node.setNodeId(nodeId);
        
        jdbcTemplate.update(getSql("registerNodeSql"), new Object[] { node.getSyncURL(), node.getSchemaVersion(),
            node.getDatabaseType(), node.getDatabaseVersion(), node.getSymmetricVersion(), node.getNodeId() },
            new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        if (Version.isOlderThanVersion(node.getSymmetricVersion(), UpgradeConstants.VERSION_FOR_NEW_REGISTRATION_PROTOCOL)) {
            markNodeAsRegistered(nodeId);
        }
        
        dataExtractorService.extractConfigurationStandalone(node, out);
        
        if (parameterService.is(ParameterConstants.AUTO_RELOAD_ENABLED)) {
            // only send automatic initial load once or if the client is really re-registering
            NodeSecurity security = nodeService.findNodeSecurity(node.getNodeId());
            if ((security != null && security.getInitialLoadTime() == null) || isRequestedRegistration) {
                dataService.reloadNode(node.getNodeId());
            }
        }
        return true;
    }
    
    @SuppressWarnings("unchecked")
    protected String getRedirectionUrlFor(String externalId) {
        List<String> list = jdbcTemplate.queryForList(getSql("getRegistrationRedirectUrlSql"), new Object[] {externalId}, new int[] {Types.VARCHAR}, String.class);
        if (list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }
    
    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo) {
        int count = jdbcTemplate.update(getSql("updateRegistrationRedirectUrlSql"), new Object[] {nodeIdToRedirectTo, externalIdToRedirect}, new int[] {Types.VARCHAR, Types.VARCHAR});
        if (count == 0) {
            jdbcTemplate.update(getSql("insertRegistrationRedirectUrlSql"), new Object[] {nodeIdToRedirectTo, externalIdToRedirect}, new int[] {Types.VARCHAR, Types.VARCHAR});
        }
    }
    
    /**
     * @see IRegistrationService#markNodeAsRegistered(Node)
     */
    @Transactional
    public void markNodeAsRegistered(String nodeId) {
        jdbcTemplate.update(getSql("registerNodeSecuritySql"), new Object[] { nodeId });

    }

    private String findNodeToRegister(String nodeGroupId, String externald) {
        return (String) jdbcTemplate.queryForObject(getSql("findNodeToRegisterSql"), new Object[] { nodeGroupId,
                externald }, String.class);
    }

    private void sleepBeforeRegistrationRetry() {
        try {
            long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND * randomTimeSlot.getRandomValueSeededByDomainId();
            logger.warn("Could not register.  Sleeping for " + sleepTimeInMs + "ms before attempting again.");
            Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
        }
    }

    public boolean isRegisteredWithServer() {
        return nodeService.findIdentity() != null;
    }

    /**
     * Client method which attempts to register with the registration.url to
     * pull configuration if the node has not already been registered. If the
     * registration server cannot be reach this method will continue to try with
     * random sleep periods up to one minute up until the registration succeeds
     * or the maximum number of attempts has been reached.
     */
    public void registerWithServer() {
        boolean registered = isRegisteredWithServer();
        int maxNumberOfAttempts = parameterService.getInt(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS);
        while (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
            try {
                logger.info("Attempting to register with " + parameterService.getRegistrationUrl());
                registered = dataLoaderService.loadData(transportManager.getRegisterTransport(new Node(
                        this.parameterService, dbDialect)));
            } catch (ConnectException e) {
                logger.warn("Connection failed while registering.");
            } catch (Exception e) {
                logger.error(e, e);
            }
            
            maxNumberOfAttempts--;

            if (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
                sleepBeforeRegistrationRetry();
            } else {
                Node node = nodeService.findIdentity();
                if (node != null) {
                    logger.info(String.format("Successfully registered node [id=%s]", node.getNodeId()));
                } else {
                    logger.error("Node registration is unavailable");
                }
            }
        }
        
        if (!registered) {
            throw new RegistrationFailedException(String.format("Failed after trying to register %s times.", parameterService.getString(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS)));
        }
    }

    /**
     * Re-open registration for a single node that already exists in the
     * database. A new password is generated and the registration_enabled flag
     * is turned on. The next node to try registering for this node group and
     * external ID will be given this information.
     */
    public void reOpenRegistration(String nodeId) {
        String password = nodeService.generatePassword();
        Node node = nodeService.findNode(nodeId);
        if (node != null) {
            int updateCount = jdbcTemplate.update(getSql("reopenRegistrationSql"), new Object[] { password, nodeId });
            if (updateCount == 0) {
                // if the update count was 0, then we probably have a row in the
                // node table, but not in node security.
                // lets go ahead and try to insert into node security.
                jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] { nodeId, password });
            } 
        } else {
            logger.warn("There was no row with a node id of " + nodeId + " to 'reopen' registration for.");
        }
    }

    /**
     * Open registration for a single new node given a node group (f.e.,
     * "STORE") and external ID (f.e., "00001"). The unique node ID and password
     * are generated and stored in the node and node_security tables with the
     * registration_enabled flag turned on. The next node to try registering for
     * this node group and external ID will be given this information.
     */
    public void openRegistration(String nodeGroup, String externalId) {
        Node me = nodeService.findIdentity();
        String nodeId = nodeService.generateNodeId(nodeGroup, externalId);
        String password = nodeService.generatePassword();
        jdbcTemplate.update(getSql("openRegistrationNodeSql"), new Object[] { nodeId, nodeGroup, externalId, me.getNodeId() });
        jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] { nodeId, password, me.getNodeId() });
        clusterService.initLockTableForNode(nodeService.findNode(nodeId));
        logger.info("Just opened registration for external id of " + externalId + " and a node group of " + nodeGroup + " and a node id of " + nodeId);
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
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

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    public void setTransportManager(ITransportManager transportManager) {
        this.transportManager = transportManager;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setRandomTimeSlot(RandomTimeSlot randomTimeSlot) {
        this.randomTimeSlot = randomTimeSlot;
    }

}
