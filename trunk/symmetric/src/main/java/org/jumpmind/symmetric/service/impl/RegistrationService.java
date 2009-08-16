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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.security.INodePasswordFilter;
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
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

/**
 * @see IRegistrationService
 */
public class RegistrationService extends AbstractService implements IRegistrationService {

    private INodeService nodeService;

    private IDataExtractorService dataExtractorService;

    private IDataService dataService;

    private IDataLoaderService dataLoaderService;

    private ITransportManager transportManager;

    private RandomTimeSlot randomTimeSlot;

    private IDbDialect dbDialect;

    private INodePasswordFilter nodePasswordFilter;

    /**
     * Register a node for the given group name and external id if the
     * registration is open.
     * 
     * @param isRequestedRegistration
     *            An indicator that registration has been requested by the
     *            remote client
     */
    public boolean registerNode(Node node, OutputStream out, boolean isRequestedRegistration) throws IOException {
        if (!nodeService.isRegistrationServer()) {
            // registration is not allowed until this node has an identity and
            // an initial load
            Node identity = nodeService.findIdentity();
            NodeSecurity security = identity == null ? null : nodeService.findNodeSecurity(identity.getNodeId());
            if (security == null || security.getInitialLoadTime() == null) {
                log.warn("RegistrationNotAllowedNoInitialLoad");
                return false;
            }
        }

        String redirectUrl = getRedirectionUrlFor(node.getExternalId());
        if (redirectUrl != null) {
            log.info("RegistrationRedirecting", node.getExternalId(), redirectUrl);
            throw new RegistrationRedirectException(redirectUrl);
        }

        String nodeId = StringUtils.isBlank(node.getNodeId()) ? nodeService.getNodeIdGenerator().selectNodeId(
                nodeService, node) : node.getNodeId();
        Node targetNode = nodeService.findNode(nodeId);
        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if ((targetNode == null || security == null || !security.isRegistrationEnabled())
                && parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED)) {
            openRegistration(node);
            nodeId = StringUtils.isBlank(node.getNodeId()) ? nodeService.getNodeIdGenerator().selectNodeId(nodeService,
                    node) : node.getNodeId();
            security = nodeService.findNodeSecurity(nodeId);
        } else if (security == null || !security.isRegistrationEnabled()) {
            return false;
        }

        node.setNodeId(nodeId);

        jdbcTemplate.update(getSql("registerNodeSql"), new Object[] { node.getSyncURL(), node.getSchemaVersion(),
                node.getDatabaseType(), node.getDatabaseVersion(), node.getSymmetricVersion(), node.getNodeId() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        if (node.getSymmetricVersion() != null
                && Version.isOlderThanVersion(node.getSymmetricVersion(),
                        UpgradeConstants.VERSION_FOR_NEW_REGISTRATION_PROTOCOL)) {
            markNodeAsRegistered(nodeId);
        }

        if (parameterService.is(ParameterConstants.AUTO_RELOAD_ENABLED)) {
            // only send automatic initial load once or if the client is really
            // re-registering
            if ((security != null && security.getInitialLoadTime() == null) || isRequestedRegistration) {
                dataService.reloadNode(node.getNodeId());
            }
        }

        dataExtractorService.extractConfigurationStandalone(node, out);

        return true;
    }

    @SuppressWarnings("unchecked")
    protected String getRedirectionUrlFor(String externalId) {
        List<String> list = jdbcTemplate.queryForList(getSql("getRegistrationRedirectUrlSql"),
                new Object[] { externalId }, new int[] { Types.VARCHAR }, String.class);
        if (list.size() > 0) {
            return transportManager.resolveURL(list.get(0));
        } else {
            return null;
        }
    }

    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo) {
        int count = jdbcTemplate.update(getSql("updateRegistrationRedirectUrlSql"), new Object[] { nodeIdToRedirectTo,
                externalIdToRedirect }, new int[] { Types.VARCHAR, Types.VARCHAR });
        if (count == 0) {
            jdbcTemplate.update(getSql("insertRegistrationRedirectUrlSql"), new Object[] { nodeIdToRedirectTo,
                    externalIdToRedirect }, new int[] { Types.VARCHAR, Types.VARCHAR });
        }
    }

    /**
     * @see IRegistrationService#markNodeAsRegistered(Node)
     */
    @Transactional
    public void markNodeAsRegistered(String nodeId) {
        jdbcTemplate.update(getSql("registerNodeSecuritySql"), new Object[] { nodeId });

    }

    public Map<String, String> getRegistrationRedirectMap() {
        SimpleJdbcTemplate template = new SimpleJdbcTemplate(this.jdbcTemplate);
        return template.queryForObject(getSql("getRegistrationRedirectSql"),
                new ParameterizedRowMapper<Map<String, String>>() {
                    public Map<String, String> mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Map<String, String> results = new HashMap<String, String>();
                        do {
                            results.put(rs.getString(1), rs.getString(2));
                        } while (rs.next());
                        return results;
                    }
                });

    }

    private void sleepBeforeRegistrationRetry() {
        try {
            long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND * randomTimeSlot.getRandomValueSeededByDomainId();
            log.warn("NodeRegistertingFailed", sleepTimeInMs);
            Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
        }
    }

    public boolean isRegisteredWithServer() {
        return nodeService.findIdentity() != null;
    }

    /**
     * @see IRegistrationService#registerWithServer()
     */
    public void registerWithServer() {
        boolean registered = isRegisteredWithServer();
        int maxNumberOfAttempts = parameterService.getInt(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS);
        while (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
            boolean errorOccurred = false;
            try {
                log.info("NodeRegisterting", parameterService.getRegistrationUrl());
                registered = dataLoaderService.loadData(transportManager.getRegisterTransport(new Node(
                        this.parameterService, dbDialect)));
            } catch (ConnectException e) {
                log.warn("NodeRegistertingFailedConnection");
            } catch (Exception e) {
                log.error(e);
            }

            maxNumberOfAttempts--;

            if (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
                sleepBeforeRegistrationRetry();
                registered = isRegisteredWithServer();
            } else {
                Node node = nodeService.findIdentity();
                if (log != null) {
                    log.info("NodeRegistered", node.getNodeId());
                } else if (!errorOccurred) {
                    log.error("NodeRegisteringFailedIdentityMissing");
                } else {
                    log.error("NodeRegisteringFailedUnavailable");
                }
            }
        }

        if (!registered) {
            throw new RegistrationFailedException(String.format("Failed after trying to register %s times.",
                    parameterService.getString(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS)));
        }
    }

    /**
     * @see IRegistrationService#reOpenRegistration(String)
     */
    public void reOpenRegistration(String nodeId) {
        Node node = nodeService.findNode(nodeId);
        String password = nodeService.getNodeIdGenerator().generatePassword(nodeService, node);
        password = filterPasswordOnSaveIfNeeded(password);
        if (node != null) {
            int updateCount = jdbcTemplate.update(getSql("reopenRegistrationSql"), new Object[] { password, nodeId });
            if (updateCount == 0) {
                // if the update count was 0, then we probably have a row in the
                // node table, but not in node security.
                // lets go ahead and try to insert into node security.
                jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] { nodeId, password });
            }
        } else {
            log.warn("NodeReregisteringFailed", nodeId);
        }
    }

    /**
     * @see IRegistrationService#openRegistration(String, String)
     */
    public void openRegistration(String nodeGroup, String externalId) {
        Node node = new Node();
        node.setExternalId(externalId);
        node.setNodeGroupId(nodeGroup);
        openRegistration(node);
    }

    public void openRegistration(Node node) {
        Node me = nodeService.findIdentity();
        if (me != null) {
            String nodeId = nodeService.getNodeIdGenerator().generateNodeId(nodeService, node);
            Node existingNode = nodeService.findNode(nodeId);
            if (existingNode == null) {
                String password = nodeService.getNodeIdGenerator().generatePassword(nodeService, node);
                password = filterPasswordOnSaveIfNeeded(password);
                jdbcTemplate.update(getSql("openRegistrationNodeSql"), new Object[] { nodeId, node.getNodeGroupId(),
                        node.getExternalId(), me.getNodeId() });
                jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] { nodeId, password,
                        me.getNodeId() });
                log.info("NodeRegistrationOpened", node.getExternalId(), node.getNodeGroupId(), nodeId);
            } else {
                reOpenRegistration(nodeId);
            }
        } else {
            throw new IllegalStateException(
                    "This node has not been configured.  Could not find a row in the identity table.");
        }
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
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

    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter) {
        this.nodePasswordFilter = nodePasswordFilter;
    }

    private String filterPasswordOnSaveIfNeeded(String password) {
        String s = password;
        if (nodePasswordFilter != null) {
            s = nodePasswordFilter.onNodeSecuritySave(password);
        }
        return s;
    }
}
