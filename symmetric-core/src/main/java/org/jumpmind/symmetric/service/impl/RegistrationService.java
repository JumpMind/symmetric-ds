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

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.INodeIdCreator;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.RegistrationRequest;
import org.jumpmind.symmetric.model.RegistrationRequest.RegistrationStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatus.Status;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.RegistrationFailedException;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRedirectException;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.RandomTimeSlot;

/**
 * @see IRegistrationService
 */
public class RegistrationService extends AbstractService implements IRegistrationService {

    private INodeService nodeService;

    private IDataExtractorService dataExtractorService;

    private IDataService dataService;

    private IDataLoaderService dataLoaderService;

    private ITransportManager transportManager;
    
    private IOutgoingBatchService outgoingBatchService;

    private RandomTimeSlot randomTimeSlot;

    private IStatisticManager statisticManager;

    private IConfigurationService configurationService;
    
    private IExtensionService extensionService;
    
    private ISymmetricEngine engine;

    public RegistrationService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;
        this.nodeService = engine.getNodeService();
        this.dataExtractorService = engine.getDataExtractorService();
        this.dataService = engine.getDataService();
        this.dataLoaderService = engine.getDataLoaderService();
        this.transportManager = engine.getTransportManager();
        this.statisticManager = engine.getStatisticManager();
        this.configurationService = engine.getConfigurationService();
        this.outgoingBatchService = engine.getOutgoingBatchService();
        this.extensionService = engine.getExtensionService();
        this.randomTimeSlot = new RandomTimeSlot(parameterService.getExternalId(), 30);
        setSqlMap(new RegistrationServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

	public Node registerPullOnlyNode(String externalId, String nodeGroupId,
			String databaseType, String databaseVersion) 
			throws IOException {
		
		Node node = new Node();
		node.setExternalId(externalId);
		node.setNodeGroupId(nodeGroupId);
		node.setDatabaseType(databaseType);
		node.setDatabaseVersion(databaseVersion);
		
		node = processRegistration(node, null, null, true, Constants.DEPLOYMENT_TYPE_REST);
		
		if (node.isSyncEnabled()) {
			//set the node as registered as we have no 
			//virtual batch for registration to be ok'd
			markNodeAsRegistered(node.getNodeId());
		}		
		return node;
	}    
    
    public boolean registerNode(Node preRegisteredNode, OutputStream out, boolean isRequestedRegistration)
            throws IOException {
        return registerNode(preRegisteredNode, null, null, out, isRequestedRegistration);
    }

    protected void extractConfiguration(OutputStream out, Node registeredNode) {
        dataExtractorService.extractConfigurationStandalone(registeredNode, out);
    }
    
    protected Node processRegistration(Node nodePriorToRegistration, String remoteHost,
            String remoteAddress, boolean isRequestedRegistration, String deploymentType)
            throws IOException {

    	Node processedNode = new Node();
    	processedNode.setSyncEnabled(false);
        Node identity = nodeService.findIdentity();
        if (identity == null) {
            RegistrationRequest req = new RegistrationRequest(nodePriorToRegistration,
                    RegistrationStatus.ER, remoteHost, remoteAddress);
            req.setErrorMessage("Cannot register a client node until this node is registered");
            saveRegistrationRequest(req);
            log.warn(req.getErrorMessage());
            return processedNode;
        }

        try {
            if (!nodeService.isRegistrationServer()) {
                /*
                 * registration is not allowed until this node has an identity
                 * and an initial load
                 */
                NodeSecurity security = nodeService.findNodeSecurity(identity.getNodeId());
                if (security == null || security.getInitialLoadTime() == null) {
                    RegistrationRequest req = new RegistrationRequest(nodePriorToRegistration,
                            RegistrationStatus.ER, remoteHost, remoteAddress);
                    req.setErrorMessage("Cannot register a client node until this node has an initial load (ie. node_security.initial_load_time is a non null value)");
                    saveRegistrationRequest(req);
                    log.warn(req.getErrorMessage());
                    return processedNode;
                }
            }

            String redirectUrl = getRedirectionUrlFor(nodePriorToRegistration.getExternalId());
            if (redirectUrl != null) {
                log.info("Redirecting {} to {} for registration.",
                        nodePriorToRegistration.getExternalId(), redirectUrl);
                saveRegistrationRequest(new RegistrationRequest(nodePriorToRegistration,
                        RegistrationStatus.RR, remoteHost, remoteAddress));
                throw new RegistrationRedirectException(redirectUrl);
            }

            /*
             * Check to see if there is a link that exists to service the node
             * that is requesting registration
             */
            NodeGroupLink link = configurationService.getNodeGroupLinkFor(
                    identity.getNodeGroupId(), nodePriorToRegistration.getNodeGroupId(), false);
            if (link == null
                    && parameterService.is(ParameterConstants.REGISTRATION_REQUIRE_NODE_GROUP_LINK,
                            true)) {
                RegistrationRequest req = new RegistrationRequest(nodePriorToRegistration,
                        RegistrationStatus.ER, remoteHost, remoteAddress);
                req.setErrorMessage(String.format("Cannot register a client node unless a node group link exists so the registering node can receive configuration updates.  Please add a group link where the source group id is %s and the target group id is %s",
                        identity.getNodeGroupId(), nodePriorToRegistration.getNodeGroupId()));
                saveRegistrationRequest(req);
                log.warn(req.getErrorMessage());
                return processedNode;
            }

            String nodeId = StringUtils.isBlank(nodePriorToRegistration.getNodeId()) ? extensionService.
                    getExtensionPoint(INodeIdCreator.class).selectNodeId(nodePriorToRegistration, remoteHost,
                            remoteAddress) : nodePriorToRegistration.getNodeId();
            
            Node foundNode = nodeService.findNode(nodeId);
            NodeSecurity security = nodeService.findNodeSecurity(nodeId);
            
            if ((foundNode == null || security == null || !security.isRegistrationEnabled())
                    && parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED)) {
                openRegistration(nodePriorToRegistration, remoteHost, remoteAddress);
                nodeId = StringUtils.isBlank(nodePriorToRegistration.getNodeId()) ? extensionService.
                        getExtensionPoint(INodeIdCreator.class).selectNodeId(nodePriorToRegistration, remoteHost,
                                remoteAddress) : nodePriorToRegistration.getNodeId();
                security = nodeService.findNodeSecurity(nodeId);
                foundNode = nodeService.findNode(nodeId);
            } else if (foundNode == null || security == null
                    || !security.isRegistrationEnabled()) {
                saveRegistrationRequest(new RegistrationRequest(nodePriorToRegistration,
                        RegistrationStatus.RQ, remoteHost, remoteAddress));
                return processedNode;
            }

            foundNode.setSyncEnabled(true);
            if (Constants.DEPLOYMENT_TYPE_REST.equalsIgnoreCase(deploymentType)) {
            	foundNode.setSymmetricVersion(null);
            	foundNode.setDeploymentType(deploymentType);
            }
            foundNode.setSyncUrl(nodePriorToRegistration.getSyncUrl());
            foundNode.setDatabaseType(nodePriorToRegistration.getDatabaseType());
            foundNode.setDatabaseVersion(nodePriorToRegistration.getDatabaseVersion());
            foundNode.setSymmetricVersion(nodePriorToRegistration.getSymmetricVersion());
            nodeService.save(foundNode);                        
            
            /**
             * Only send automatic initial load once or if the client is really
             * re-registering
             */
            if ((security != null && security.getInitialLoadTime() == null)
                    || isRequestedRegistration) {
                if (parameterService.is(ParameterConstants.AUTO_RELOAD_ENABLED)) {
                    nodeService.setInitialLoadEnabled(nodeId, true, false, -1, "registration");
                }

                if (parameterService.is(ParameterConstants.AUTO_RELOAD_REVERSE_ENABLED)) {
                    nodeService.setReverseInitialLoadEnabled(nodeId, true, false, -1,
                            "registration");
                }
            }
            
            saveRegistrationRequest(new RegistrationRequest(foundNode, RegistrationStatus.OK,
                    remoteHost, remoteAddress));

            statisticManager.incrementNodesRegistered(1);

           return foundNode;
            
        } catch (RegistrationNotOpenException ex) {
            if (StringUtils.isNotBlank(ex.getMessage())) {
                log.warn("Registration not allowed for {} because {}",
                        nodePriorToRegistration.toString(), ex.getMessage());
            }
            return processedNode;
        }
    }
    
    /**
     * @see IRegistrationService#registerNode(Node, OutputStream, boolean)
     */
    public boolean registerNode(Node nodePriorToRegistration, String remoteHost,
            String remoteAddress, OutputStream out, boolean isRequestedRegistration)
            throws IOException {

        Node processedNode = processRegistration(nodePriorToRegistration, remoteHost,
                remoteAddress, isRequestedRegistration, null);

        if (processedNode.isSyncEnabled()) {
            /*
             * Mark all configuration batches as processed because we are about to reload
             * the configuration for the node
             */
            outgoingBatchService.markAllConfigAsSentForNode(processedNode.getNodeId());
        	extractConfiguration(out, processedNode);
        }
        
        return processedNode.isSyncEnabled();
    }

    public List<RegistrationRequest> getRegistrationRequests(
            boolean includeNodesWithOpenRegistrations) {
        List<RegistrationRequest> requests = sqlTemplate.query(
                getSql("selectRegistrationRequestSql"), new RegistrationRequestMapper());
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

    public boolean deleteRegistrationRequest(RegistrationRequest request) {
        String externalId = request.getExternalId() == null ? "" : request.getExternalId();
        String nodeGroupId = request.getNodeGroupId() == null ? "" : request.getNodeGroupId();
        return 0 < sqlTemplate.update(getSql("deleteRegistrationRequestSql"), new Object[] {
                nodeGroupId, externalId, request.getIpAddress(), request.getHostName(),
                request.getStatus().name() });
    }

    public void saveRegistrationRequest(RegistrationRequest request) {
        /**
         * Lookup existing registration requests to update the attempt count.  We previously
         * did this in SQL on the update, but as400 v5 didn't like that
         */
        boolean foundOne = false;
        List<RegistrationRequest> requests = getRegistrationRequests(true);
        for (RegistrationRequest registrationRequest : requests) {
            if (registrationRequest.getNodeGroupId().equals(request.getNodeGroupId()) && registrationRequest.getExternalId().equals(request.getExternalId())) {
                request.setAttemptCount(registrationRequest.getAttemptCount()+1);
                foundOne = true;
                break;
            }
        }
        String externalId = request.getExternalId() == null ? "" : request.getExternalId();
        String nodeGroupId = request.getNodeGroupId() == null ? "" : request.getNodeGroupId();
        int count = 0;
        if (foundOne) {
            count = sqlTemplate.update(
                    getSql("updateRegistrationRequestSql"),
                    new Object[] { request.getAttemptCount(), request.getLastUpdateBy(),
                            request.getLastUpdateTime(), request.getRegisteredNodeId(),
                            request.getStatus().name(), request.getErrorMessage(), nodeGroupId,
                            externalId, request.getIpAddress(), request.getHostName() }, new int[] {
                            Types.NUMERIC, Types.VARCHAR, Types.DATE, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR });
        }
        
        if (count == 0) {
            sqlTemplate.update(
                    getSql("insertRegistrationRequestSql"),
                    new Object[] { request.getLastUpdateBy(), request.getLastUpdateTime(),
                            request.getRegisteredNodeId(), request.getStatus().name(), nodeGroupId,
                            externalId, request.getIpAddress(), request.getHostName(),
                            request.getErrorMessage() }, new int[] { Types.VARCHAR, Types.DATE,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        }

    }

    public String getRedirectionUrlFor(String externalId) {
        List<String> list = sqlTemplate.query(getSql("getRegistrationRedirectUrlSql"),
                new StringMapper(), new Object[] { externalId }, new int[] { Types.VARCHAR });
        if (list.size() > 0) {
            return transportManager.resolveURL(list.get(0), parameterService.getRegistrationUrl());
        } else {
            return null;
        }
    }

    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo) {
        int count = sqlTemplate.update(getSql("updateRegistrationRedirectUrlSql"), new Object[] {
                nodeIdToRedirectTo, externalIdToRedirect }, new int[] { Types.VARCHAR,
                Types.VARCHAR });
        if (count == 0) {
            sqlTemplate.update(getSql("insertRegistrationRedirectUrlSql"), new Object[] {
                    nodeIdToRedirectTo, externalIdToRedirect }, new int[] { Types.VARCHAR,
                    Types.VARCHAR });
        }
    }

    /**
     * @see IRegistrationService#markNodeAsRegistered(Node)
     */
    public void markNodeAsRegistered(String nodeId) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            symmetricDialect.disableSyncTriggers(transaction, nodeId);
            transaction.prepareAndExecute(getSql("registerNodeSecuritySql"), nodeId);
            transaction.commit();
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } finally {
            symmetricDialect.enableSyncTriggers(transaction);
            close(transaction);
        }
    }

    private void sleepBeforeRegistrationRetry() {
        long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND
                * randomTimeSlot.getRandomValueSeededByExternalId();
        log.info("Could not register.  Sleeping before attempting again.", sleepTimeInMs);
        log.info("Sleeping for {}ms", sleepTimeInMs);
        AppUtils.sleep(sleepTimeInMs);
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
        while (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0) && engine.isStarted()) {
            try {
                log.info("This node is unregistered.  It will attempt to register using the registration.url");
                registered = dataLoaderService.loadDataFromPull(null).getStatus() == Status.DATA_PROCESSED;
            } catch (ConnectException e) {
                log.warn("The request to register failed because the client failed to connect to the server");
            } catch (UnknownHostException e) {
                log.warn("The request to register failed because the host was unknown");
            } catch (ConnectionRejectedException ex) {
                log.warn("The request to register was rejected by the server.  Either the server node is not started, the server is not configured properly or the registration url is incorrect");
            } catch (Exception e) {
                log.error("", e);
            }

            maxNumberOfAttempts--;

            if (!registered && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
                registered = isRegisteredWithServer();
                if (registered) {
                    log.info("We registered, but were not able to acknowledge our registration.  Sending a sql event to the node where we registered to indicate that we are alive and registered");
                    Node identity = nodeService.findIdentity();
                    Node parentNode = nodeService.findNode(identity.getCreatedAtNodeId());
                    dataService
                            .insertSqlEvent(
                                    parentNode,
                                    "update "
                                            + tablePrefix
                                            + "_node_security set registration_enabled=1, registration_time=current_timestamp where node_id='"
                                            + identity.getNodeId() + "'", false, -1, null);
                }
            }

            if (registered) {
                Node node = nodeService.findIdentity();
                if (node != null) {
                    log.info("Successfully registered node [id={}]", node.getNodeId());
                    extensionService.refresh();
                    dataService.heartbeat(true);
                } else {
                    log.error("Node identity is missing after registration.  The registration server may be misconfigured or have an error");
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
        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        String password = null;
        if (security != null && parameterService.is(ParameterConstants.REGISTRATION_REOPEN_USE_SAME_PASSWORD, true)) {
            password = security.getNodePassword();
        } else {
            password = extensionService.getExtensionPoint(INodeIdCreator.class).generatePassword(node);
            password = filterPasswordOnSaveIfNeeded(password);
        }
        if (node != null) {
            int updateCount = sqlTemplate.update(getSql("reopenRegistrationSql"), new Object[] {
                    password, nodeId });
            if (updateCount == 0 && nodeService.findNodeSecurity(nodeId) == null) {
                // if the update count was 0, then we probably have a row in the
                // node table, but not in node security.
                // lets go ahead and try to insert into node security.
                sqlTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] {
                        nodeId, password, nodeService.findNode(nodeId).getNodeId() });
                log.info("Registration was opened for {}", nodeId);
            } else if (updateCount == 0) {
                log.warn("Registration was already enabled for {}.  No need to reenable it", nodeId);
            } else {
                log.info("Registration was reopened for {}", nodeId);
            }
        } else {
            log.warn("There was no row with a node id of {} to 'reopen' registration for", nodeId);
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
    
    public synchronized String openRegistration(String nodeGroup, String externalId, String remoteHost, String remoteAddress) {
        Node node = new Node();
        node.setExternalId(externalId);
        node.setNodeGroupId(nodeGroup);
        return openRegistration(node, remoteHost, remoteAddress);
    }

    public synchronized String openRegistration(Node node) {
        return openRegistration(node, null, null);
    }

    protected String openRegistration(Node node, String remoteHost, String remoteAddress) {
        Node me = nodeService.findIdentity();
        if (me != null) {
            String nodeId = extensionService.getExtensionPoint(INodeIdCreator.class).generateNodeId(node, remoteHost, remoteAddress);
            Node existingNode = nodeService.findNode(nodeId);
            if (existingNode == null) {
                node.setNodeId(nodeId);
                node.setSyncEnabled(false);
                
                boolean masterToMasterOnly = configurationService.isMasterToMasterOnly();
                node.setCreatedAtNodeId(masterToMasterOnly ? null: me.getNodeId());
                nodeService.save(node);

                // make sure there isn't a node security row lying around w/out
                // a node row
                nodeService.deleteNodeSecurity(nodeId);
                String password = extensionService.getExtensionPoint(INodeIdCreator.class).generatePassword(node);
                password = filterPasswordOnSaveIfNeeded(password);
                sqlTemplate.update(getSql("openRegistrationNodeSecuritySql"), new Object[] {
                        nodeId, password, masterToMasterOnly ? null : me.getNodeId() });
                nodeService.insertNodeGroup(node.getNodeGroupId(), null);
                log.info(
                        "Just opened registration for external id of {} and a node group of {} and a node id of {}",
                        new Object[] { node.getExternalId(), node.getNodeGroupId(), nodeId });
            } else {
                reOpenRegistration(nodeId);
            }
            return nodeId;
        } else {
            throw new IllegalStateException(
                    "This node has not been configured.  Could not find a row in the identity table");
        }
    }

    public boolean isAutoRegistration() {
        return parameterService.is(ParameterConstants.AUTO_REGISTER_ENABLED);
    }

    private String filterPasswordOnSaveIfNeeded(String password) {
        String s = password;
        INodePasswordFilter nodePasswordFilter = extensionService.getExtensionPoint(INodePasswordFilter.class);
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
    
    public void requestNodeCopy() {
        Node copyFrom = nodeService.findIdentity();
        if (copyFrom == null) {
            throw new IllegalStateException("No identity found.  Can only copy if the node has an identity");
        }
        boolean copied = false;
        int maxNumberOfAttempts = parameterService
                .getInt(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS);
        while (!copied && (maxNumberOfAttempts < 0 || maxNumberOfAttempts > 0)) {
            
            try {
                log.info("Detected that node '{}' should be copied to a new node id.  Attempting to contact server to accomplish this", copyFrom.getNodeId());
                 copied = transportManager.sendCopyRequest(copyFrom) == HttpURLConnection.HTTP_OK;   
                 if (copied) {
                     nodeService.deleteIdentity();
                 }
            } catch (ConnectException e) {
                log.warn("The request to copy failed because the client failed to connect to the server");
            } catch (UnknownHostException e) {
                log.warn("The request to copy failed because the host was unknown");
            } catch (ConnectionRejectedException ex) {
                log.warn("The request to copy was rejected by the server.  Either the server node is not started, the server is not configured properly or the registration url is incorrect");
            } catch (Exception e) {
                log.error("", e);
            }

            maxNumberOfAttempts--;
            
            if (!copied) {
                long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND
                        * randomTimeSlot.getRandomValueSeededByExternalId();
                log.warn("Copy failed.  Sleeping before attempting again.", sleepTimeInMs);
                log.info("Sleeping for {}ms", sleepTimeInMs);
                AppUtils.sleep(sleepTimeInMs);
            }

        }

        if (!copied) {
            throw new RegistrationFailedException(String.format(
                    "Failed after trying to copy %s times.",
                    parameterService.getString(ParameterConstants.REGISTRATION_NUMBER_OF_ATTEMPTS)));
        }        
    }

    class RegistrationRequestMapper implements ISqlRowMapper<RegistrationRequest> {
        public RegistrationRequest mapRow(Row rs) {
            RegistrationRequest request = new RegistrationRequest();
            request.setNodeGroupId(rs.getString("node_group_id"));
            request.setExternalId(rs.getString("external_id"));
            request.setStatus(RegistrationStatus.valueOf(RegistrationStatus.class,
                    rs.getString("status")));
            request.setHostName(rs.getString("host_name"));
            request.setIpAddress(rs.getString("ip_address"));
            request.setAttemptCount(rs.getLong("attempt_count"));
            request.setRegisteredNodeId(rs.getString("registered_node_id"));
            request.setCreateTime(rs.getDateTime("create_time"));
            request.setLastUpdateBy(rs.getString("last_update_by"));
            request.setLastUpdateTime(rs.getDateTime("last_update_time"));
            request.setErrorMessage(rs.getString("error_message"));
            return request;
        }
    }

}
