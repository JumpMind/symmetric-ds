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
 * under the License. 
 */
package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.RegistrationRequest;
import org.jumpmind.symmetric.model.RegistrationRequest.RegistrationStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatus.Status;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.RegistrationFailedException;
import org.jumpmind.symmetric.service.RegistrationRedirectException;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.upgrade.UpgradeConstants;
import org.jumpmind.symmetric.util.RandomTimeSlot;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

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

    private INodePasswordFilter nodePasswordFilter;
    
    private IStatisticManager statisticManager;

    public boolean registerNode(Node node, OutputStream out, boolean isRequestedRegistration)
            throws IOException {
        return registerNode(node, null, null, out, isRequestedRegistration);
    }

    /**
     * @see IRegistrationService#registerNode(Node, OutputStream, boolean)
     */
    public boolean registerNode(Node node, String remoteHost, String remoteAddress,
            OutputStream out, boolean isRequestedRegistration) throws IOException {
        if (!nodeService.isRegistrationServer()) {
            // registration is not allowed until this node has an identity and
            // an initial load
            Node identity = nodeService.findIdentity();
            NodeSecurity security = identity == null ? null : nodeService.findNodeSecurity(identity
                    .getNodeId());
            if (security == null || security.getInitialLoadTime() == null) {
                saveRegisgtrationRequest(new RegistrationRequest(node, RegistrationStatus.RQ,
                        remoteHost, remoteAddress));
                log.warn("RegistrationNotAllowedNoInitialLoad");
                return false;
            }
        }

        String redirectUrl = getRedirectionUrlFor(node.getExternalId());
        if (redirectUrl != null) {
            log.info("RegistrationRedirecting", node.getExternalId(), redirectUrl);
            saveRegisgtrationRequest(new RegistrationRequest(node, RegistrationStatus.RR,
                    remoteHost, remoteAddress));
            throw new RegistrationRedirectException(redirectUrl);
        }

        String nodeId = StringUtils.isBlank(node.getNodeId()) ? nodeService.getNodeIdGenerator()
                .selectNodeId(nodeService, node) : node.getNodeId();
        Node targetNode = nodeService.findNode(nodeId);
        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if ((targetNode == null || security == null || !security.isRegistrationEnabled())
                && parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED)) {
            openRegistration(node);
            nodeId = StringUtils.isBlank(node.getNodeId()) ? nodeService.getNodeIdGenerator()
                    .selectNodeId(nodeService, node) : node.getNodeId();
            security = nodeService.findNodeSecurity(nodeId);
        } else if (targetNode == null || security == null || !security.isRegistrationEnabled()) {
            saveRegisgtrationRequest(new RegistrationRequest(node, RegistrationStatus.RQ,
                    remoteHost, remoteAddress));
            return false;
        }

        node.setNodeId(nodeId);

        jdbcTemplate.update(getSql("registerNodeSql"),
                new Object[] { node.getSyncUrl(), node.getSchemaVersion(), node.getDatabaseType(),
                        node.getDatabaseVersion(), node.getSymmetricVersion(), node.getNodeId() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR });

        if (node.getSymmetricVersion() != null
                && Version.isOlderThanVersion(node.getSymmetricVersion(),
                        UpgradeConstants.VERSION_FOR_NEW_REGISTRATION_PROTOCOL)) {
            markNodeAsRegistered(nodeId);
        }

        if (parameterService.is(ParameterConstants.AUTO_RELOAD_ENABLED)) {
            // only send automatic initial load once or if the client is really
            // re-registering
            if ((security != null && security.getInitialLoadTime() == null)
                    || isRequestedRegistration) {
                dataService.reloadNode(node.getNodeId());
            }
        }

        dataExtractorService.extractConfigurationStandalone(node, out);

        saveRegisgtrationRequest(new RegistrationRequest(node, RegistrationStatus.OK, remoteHost,
                remoteAddress));

        statisticManager.incrementNodesRegistered(1);
        
        return true;
    }

    public List<RegistrationRequest> getRegistrationRequests(
            boolean includeNodesWithOpenRegistrations) {
        List<RegistrationRequest> requests = jdbcTemplate.query(
                getSql("selectRegistrationRequestSql"), new RegistrationRequestMapper(),
                RegistrationStatus.RQ.name());
        if (!includeNodesWithOpenRegistrations) {
            Collection<Node> nodes = nodeService.findNodesWithOpenRegistration();
            Iterator<RegistrationRequest> i = requests.iterator();
            while (i.hasNext()) {
                RegistrationRequest registrationRequest = (RegistrationRequest) i.next();
                for (Node node : nodes) {
                    if (node.getNodeGroupId().equals(registrationRequest.getNodeGroupId())
                            && node.getExternalId().equals(registrationRequest.getExternalId())) {
                        i.remove();
                    }
                }
            }
        }
        return requests;
    }

    public void saveRegisgtrationRequest(RegistrationRequest request) {
        String externalId = request.getExternalId() == null ? "" : request.getExternalId();
        String nodeGroupId = request.getNodeGroupId() == null ? "" : request.getNodeGroupId();
        int count = jdbcTemplate.update(
                getSql("updateRegistrationRequestSql"),
                new Object[] { request.getLastUpdateBy(), request.getLastUpdateTime(),
                        request.getRegisteredNodeId(), request.getStatus().name(), nodeGroupId,
                        externalId, request.getIpAddress(), request.getHostName(),
                        RegistrationStatus.RQ.name() });
        if (count == 0) {
            jdbcTemplate.update(
                    getSql("insertRegistrationRequestSql"),
                    new Object[] { request.getLastUpdateBy(), request.getLastUpdateTime(),
                            request.getRegisteredNodeId(), request.getStatus().name(), nodeGroupId,
                            externalId, request.getIpAddress(), request.getHostName() });
        }

    }

    public String getRedirectionUrlFor(String externalId) {
        List<String> list = jdbcTemplate.queryForList(getSql("getRegistrationRedirectUrlSql"),
                new Object[] { externalId }, new int[] { Types.VARCHAR }, String.class);
        if (list.size() > 0) {
            return transportManager.resolveURL(list.get(0), parameterService.getRegistrationUrl());
        } else {
            return null;
        }
    }

    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo) {
        int count = jdbcTemplate.update(getSql("updateRegistrationRedirectUrlSql"), new Object[] {
                nodeIdToRedirectTo, externalIdToRedirect }, new int[] { Types.VARCHAR,
                Types.VARCHAR });
        if (count == 0) {
            jdbcTemplate.update(getSql("insertRegistrationRedirectUrlSql"), new Object[] {
                    nodeIdToRedirectTo, externalIdToRedirect }, new int[] { Types.VARCHAR,
                    Types.VARCHAR });
        }
    }

    /**
     * @see IRegistrationService#markNodeAsRegistered(Node)
     */
    public void markNodeAsRegistered(String nodeId) {
        jdbcTemplate.update(getSql("registerNodeSecuritySql"), new Object[] { nodeId });
    }

    public Map<String, String> getRegistrationRedirectMap() {
        return this.jdbcTemplate.query(getSql("getRegistrationRedirectSql"), new Object[0],
                new ResultSetExtractor<Map<String, String>>() {
                    public Map<String, String> extractData(ResultSet rs) throws SQLException,
                            DataAccessException {
                        Map<String, String> results = new HashMap<String, String>();
                        while (rs.next()) {
                            results.put(rs.getString(1), rs.getString(2));
                        }
                        ;
                        return results;
                    }
                });
    }

    private void sleepBeforeRegistrationRetry() {
        try {
            long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND
                    * randomTimeSlot.getRandomValueSeededByExternalId();
            log.warn("NodeRegisteringFailed", sleepTimeInMs);
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
        int maxNumberOfAttempts = parameterService
                .getInt(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS);
        while (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
            boolean errorOccurred = false;
            try {
                log.info("NodeRegistering");
                registered = dataLoaderService.loadDataFromPull(null).getStatus() == Status.DATA_PROCESSED;
            } catch (ConnectException e) {
                log.warn("NodeRegisteringFailedConnection");
            } catch (UnknownHostException e) {
                log.warn("NodeRegisteringFailedConnection");
            } catch (Exception e) {
                log.error(e);
            }

            maxNumberOfAttempts--;

            if (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
                registered = isRegisteredWithServer();
            } else {
                Node node = nodeService.findIdentity();
                if (node != null) {
                    log.info("NodeRegistered", node.getNodeId());
                } else if (!errorOccurred) {
                    log.error("NodeRegisteringFailedIdentityMissing");
                    registered = false;
                } else {
                    log.error("NodeRegisteringFailedUnavailable");
                    registered = false;
                }
            }
            
            if (!registered && maxNumberOfAttempts != 0) {
                sleepBeforeRegistrationRetry();
            }
        }

        if (!registered) {
            throw new RegistrationFailedException(String.format(
                    "Failed after trying to register %s times.",
                    parameterService.getString(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS)));
        }
    }

    /**
     * @see IRegistrationService#reOpenRegistration(String)
     */
    public synchronized void reOpenRegistration(String nodeId) {
        Node node = nodeService.findNode(nodeId);
        String password = nodeService.getNodeIdGenerator().generatePassword(nodeService, node);
        password = filterPasswordOnSaveIfNeeded(password);
        if (node != null) {
            int updateCount = jdbcTemplate.update(getSql("reopenRegistrationSql"), new Object[] {
                    password, nodeId });
            if (updateCount == 0) {
                // if the update count was 0, then we probably have a row in the
                // node table, but not in node security.
                // lets go ahead and try to insert into node security.
                jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] {
                        nodeId, password, nodeService.findNode(nodeId).getNodeId() });
            }
        } else {
            log.warn("NodeReregisteringFailed", nodeId);
        }
    }

    /**
     * @see IRegistrationService#openRegistration(String, String)
     * @return The nodeId of the registered node
     */
    public synchronized String openRegistration(String nodeGroup, String externalId) {
        Node node = new Node();
        node.setExternalId(externalId);
        node.setNodeGroupId(nodeGroup);
        return openRegistration(node);
    }

    protected synchronized String openRegistration(Node node) {
        Node me = nodeService.findIdentity();
        if (me != null
                || (parameterService.getExternalId().equals(node.getExternalId()) && parameterService
                        .getNodeGroupId().equals(node.getNodeGroupId()))) {
            String nodeId = nodeService.getNodeIdGenerator().generateNodeId(nodeService, node);
            Node existingNode = nodeService.findNode(nodeId);
            if (existingNode == null) {
                // make sure there isn't a node security row lying around w/out a node row
                nodeService.deleteNodeSecurity(nodeId);
                String password = nodeService.getNodeIdGenerator().generatePassword(nodeService,
                        node);
                password = filterPasswordOnSaveIfNeeded(password);
                nodeService.insertNode(nodeId, node.getNodeGroupId(), node.getExternalId(),
                        me.getNodeId());
                jdbcTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] {
                        nodeId, password, me.getNodeId() });
                nodeService.insertNodeGroup(node.getNodeGroupId(), null);
                log.info("NodeRegistrationOpened", node.getExternalId(), node.getNodeGroupId(),
                        nodeId);
            } else {
                reOpenRegistration(nodeId);
            }
            return nodeId;
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

    public void setRandomTimeSlot(RandomTimeSlot randomTimeSlot) {
        this.randomTimeSlot = randomTimeSlot;
    }

    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter) {
        this.nodePasswordFilter = nodePasswordFilter;
    }
    
    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    private String filterPasswordOnSaveIfNeeded(String password) {
        String s = password;
        if (nodePasswordFilter != null) {
            s = nodePasswordFilter.onNodeSecuritySave(password);
        }
        return s;
    }
    
    public boolean isRegistrationOpen(String nodeGroupId, String externalId) {
        Node node = nodeService.findNodeByExternalId(nodeGroupId, externalId);
        if (node != null) {
            NodeSecurity security = nodeService.findNodeSecurity(node.getNodeId());
            return security != null && security.isRegistrationEnabled();
        }
        return false;
    }

    class RegistrationRequestMapper implements RowMapper<RegistrationRequest> {
        public RegistrationRequest mapRow(ResultSet rs, int rowNum) throws SQLException {
            RegistrationRequest request = new RegistrationRequest();
            request.setNodeGroupId(rs.getString(1));
            request.setExternalId(rs.getString(2));
            request.setStatus(RegistrationStatus.valueOf(RegistrationStatus.class, rs.getString(3)));
            request.setHostName(rs.getString(4));
            request.setIpAddress(rs.getString(5));
            request.setAttemptCount(rs.getLong(6));
            request.setRegisteredNodeId(rs.getString(7));
            request.setCreateTime(rs.getTimestamp(8));
            request.setLastUpdateBy(rs.getString(7));
            request.setLastUpdateTime(rs.getTimestamp(8));
            return request;
        }
    }
}