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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DatabaseParameter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.RandomTimeSlot;

public class NodeCommunicationService extends AbstractService implements INodeCommunicationService {

    private Map<CommunicationType, ThreadPoolExecutor> executors = new HashMap<NodeCommunication.CommunicationType, ThreadPoolExecutor>();

    private INodeService nodeService;
    
    private IClusterService clusterService;

    private boolean initialized = false;
    
    private Map<CommunicationType, Set<String>> currentlyExecuting;

    public NodeCommunicationService(IClusterService clusterService, INodeService nodeService, IParameterService parameterService,
            ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
        setSqlMap(new NodeCommunicationServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        this.currentlyExecuting = new HashMap<NodeCommunication.CommunicationType, Set<String>>();
        CommunicationType[] types = CommunicationType.values();
        for (CommunicationType communicationType : types) {
            this.currentlyExecuting.put(communicationType, new HashSet<String>());
        }
    }

    private final void initialize() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    try {
                        int locksCleared = sqlTemplate.update(getSql("clearLocksOnRestartSql"),
                                clusterService.getServerId());
                        if (locksCleared > 0) {
                            log.info("Cleared {} node communication locks for {}", locksCleared,
                                    clusterService.getServerId());
                        }
                    } finally {
                        initialized = true;
                    }
                }
            }
        }
    }
    
    public NodeCommunication find(String nodeId, CommunicationType communicationType) {
        NodeCommunication lock = sqlTemplate.queryForObject(
                getSql("selectNodeCommunicationByNodeIdSql"), new NodeCommunicationMapper(),
                nodeId, communicationType.name());
        if (lock == null) {
            lock = new NodeCommunication();
            lock.setNodeId(nodeId);
            lock.setCommunicationType(communicationType);
            save(lock);
        }
        return lock;
    }

    public List<NodeCommunication> list(CommunicationType communicationType) {
        initialize();
        List<NodeCommunication> communicationRows = new ArrayList<NodeCommunication>(
                sqlTemplate.query(getSql("selectNodeCommunicationSql"),
                        new NodeCommunicationMapper(), communicationType.name()));
        List<Node> nodesToCommunicateWith = null;
        switch (communicationType) {
            case PULL:
            case FILE_PULL:
                nodesToCommunicateWith = removeOfflineNodes(nodeService.findNodesToPull());
                break;
            case FILE_PUSH:
            case PUSH:
                nodesToCommunicateWith = removeOfflineNodes(nodeService.findNodesToPushTo());
                break;
            case OFFLN_PUSH:
                nodesToCommunicateWith = getNodesToCommunicateWithOffline(CommunicationType.PUSH);
                break;
            case OFFLN_PULL:
                nodesToCommunicateWith = getNodesToCommunicateWithOffline(CommunicationType.PULL);
                break;
            default:
                nodesToCommunicateWith = new ArrayList<Node>(0);
                break;
        }

        for (Node nodeToCommunicateWith : nodesToCommunicateWith) {
            NodeCommunication comm = null;
            for (NodeCommunication nodeCommunication : communicationRows) {
                if (nodeCommunication.getNodeId().equals(nodeToCommunicateWith.getNodeId())) {
                    comm = nodeCommunication;
                    break;
                }
            }

            if (comm == null) {
                comm = new NodeCommunication();
                comm.setNodeId(nodeToCommunicateWith.getNodeId());
                comm.setCommunicationType(communicationType);
                save(comm);
                communicationRows.add(comm);
            }

            comm.setNode(nodeToCommunicateWith);
        }

        Iterator<NodeCommunication> it = communicationRows.iterator();
        while (it.hasNext()) {
            NodeCommunication nodeCommunication = it.next();

            Node node = null;
            for (Node nodeToCommunicateWith : nodesToCommunicateWith) {
                if (nodeCommunication.getNodeId().equals(nodeToCommunicateWith.getNodeId())) {
                    node = nodeToCommunicateWith;
                    break;
                }
            }

            if (node == null) {
                delete(nodeCommunication);
                it.remove();
            }
        }

        return communicationRows;
    }

    protected List<Node> removeOfflineNodes(List<Node> nodes) {
        if (parameterService.is(ParameterConstants.NODE_OFFLINE)) {
            nodes.clear();
        } else {
            List<DatabaseParameter> parms = parameterService.getDatabaseParametersFor(ParameterConstants.NODE_OFFLINE);
            for (DatabaseParameter parm : parms) {
                Iterator<Node> iter = nodes.iterator();
                while (iter.hasNext()) {
                    Node node = iter.next();
                    if ((parm.getNodeGroupId().equals(ParameterConstants.ALL) || parm.getNodeGroupId().equals(node.getNodeGroupId()) &&
                            (parm.getExternalId().equals(ParameterConstants.ALL) || parm.getExternalId().equals(node.getExternalId())))) {
                        iter.remove();
                    }
                }
            }
        }
        return nodes;
    }

    protected List<Node> getNodesToCommunicateWithOffline(CommunicationType communicationType) {
        List<Node> nodesToCommunicateWith = null;
        if (parameterService.is(ParameterConstants.NODE_OFFLINE) || 
                (communicationType.equals(CommunicationType.PULL) && parameterService.is(ParameterConstants.NODE_OFFLINE_INCOMING_ACCEPT_ALL))) {            
            if (communicationType.equals(CommunicationType.PUSH)) {
                nodesToCommunicateWith = nodeService.findTargetNodesFor(NodeGroupLinkAction.W);
                nodesToCommunicateWith.addAll(nodeService.findNodesToPushTo());
            } else if (communicationType.equals(CommunicationType.PULL)) {
                nodesToCommunicateWith = nodeService.findSourceNodesFor(NodeGroupLinkAction.P);
                nodesToCommunicateWith.addAll(nodeService.findNodesToPull());
            }
        } else {
            List<DatabaseParameter> parms = parameterService.getDatabaseParametersFor(ParameterConstants.NODE_OFFLINE);
            nodesToCommunicateWith = new ArrayList<Node>(parms.size());
            if (parms.size() > 0) {
                List<Node> sourceNodes = null;
                if (communicationType.equals(CommunicationType.PUSH)) {
                    sourceNodes = nodeService.findTargetNodesFor(NodeGroupLinkAction.W);
                    sourceNodes.addAll(nodeService.findNodesToPushTo());
                } else if (communicationType.equals(CommunicationType.PULL)) {
                    sourceNodes = nodeService.findSourceNodesFor(NodeGroupLinkAction.P);
                    sourceNodes.addAll(nodeService.findNodesToPull());
                }
                if (sourceNodes != null && sourceNodes.size() > 0) {
                    for (DatabaseParameter parm : parms) {
                        for (Node node : sourceNodes) {
                            if ((parm.getNodeGroupId().equals(ParameterConstants.ALL) || parm.getNodeGroupId().equals(node.getNodeGroupId()) &&
                                    (parm.getExternalId().equals(ParameterConstants.ALL) || parm.getExternalId().equals(node.getExternalId())))) {
                                nodesToCommunicateWith.add(node);
                            }
                        }
                    }
                }
            }
        }
        return nodesToCommunicateWith;
    }

    public boolean delete(NodeCommunication nodeCommunication) {
        return 1 == sqlTemplate.update(getSql("deleteNodeCommunicationSql"),
                nodeCommunication.getNodeId(), nodeCommunication.getCommunicationType().name());
    }

    public void save(NodeCommunication nodeCommunication) {
        if (0 == sqlTemplate.update(getSql("updateNodeCommunicationSql"),
                nodeCommunication.getLockTime(), nodeCommunication.getLockingServerId(),
                nodeCommunication.getLastLockMillis(), nodeCommunication.getSuccessCount(),
                nodeCommunication.getFailCount(), nodeCommunication.getTotalSuccessCount(),
                nodeCommunication.getTotalFailCount(), nodeCommunication.getTotalSuccessMillis(),
                nodeCommunication.getTotalFailMillis(), nodeCommunication.getLastLockTime(),
                nodeCommunication.getNodeId(), nodeCommunication.getCommunicationType().name())) {
            sqlTemplate.update(getSql("insertNodeCommunicationSql"),
                    nodeCommunication.getLockTime(), nodeCommunication.getLockingServerId(),
                    nodeCommunication.getLastLockMillis(), nodeCommunication.getSuccessCount(),
                    nodeCommunication.getFailCount(), nodeCommunication.getTotalSuccessCount(),
                    nodeCommunication.getTotalFailCount(),
                    nodeCommunication.getTotalSuccessMillis(),
                    nodeCommunication.getTotalFailMillis(), nodeCommunication.getLastLockTime(),
                    nodeCommunication.getNodeId(), nodeCommunication.getCommunicationType().name());
        }
    }

    protected ThreadPoolExecutor getExecutor(final CommunicationType communicationType) {
        ThreadPoolExecutor service = executors.get(communicationType);
        
        String threadCountParameter = "";
        switch (communicationType) {
            case PULL:
                threadCountParameter = ParameterConstants.PULL_THREAD_COUNT_PER_SERVER;
                break;
            case PUSH:
                threadCountParameter = ParameterConstants.PUSH_THREAD_COUNT_PER_SERVER;
                break;
            case OFFLN_PULL:
                threadCountParameter = ParameterConstants.OFFLINE_PULL_THREAD_COUNT_PER_SERVER;
                break;
            case OFFLN_PUSH:
                threadCountParameter = ParameterConstants.OFFLINE_PUSH_THREAD_COUNT_PER_SERVER;
                break;
            case FILE_PULL:
                threadCountParameter = ParameterConstants.FILE_PUSH_THREAD_COUNT_PER_SERVER;
                break;
            case FILE_PUSH:
                threadCountParameter = ParameterConstants.FILE_PUSH_THREAD_COUNT_PER_SERVER;
                break;
            case EXTRACT:
                threadCountParameter = ParameterConstants.INITIAL_LOAD_EXTRACT_THREAD_COUNT_PER_SERVER;                
                break;
            default:
                break;
        }
        int threadCount = parameterService.getInt(threadCountParameter, 1);
        
        if (service != null && service.getCorePoolSize() != threadCount) {
            log.info("{} has changed from {} to {}.  Restarting thread pool", new Object[] { threadCountParameter, service.getCorePoolSize(), threadCount });
            stop();
            service = null;
        }
        
        if (service == null) {
            synchronized (this) {
                service = executors.get(communicationType);
                if (service == null) {
                    if (threadCount <= 0) {
                        log.warn("{}={} is not a valid value. Defaulting to 1",
                                threadCountParameter, threadCount);
                        threadCount = 1;
                    } else if (threadCount > 1) {
                        log.info("{} will use {} threads", communicationType.name().toLowerCase(),
                                threadCount);
                    }
                    service = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount,
                            new ThreadFactory() {
                                final AtomicInteger threadNumber = new AtomicInteger(1);
                                final String namePrefix = parameterService.getEngineName()
                                        .toLowerCase()
                                        + "-"
                                        + communicationType.name().toLowerCase() + "-";

                                public Thread newThread(Runnable r) {
                                    Thread t = new Thread(r);
                                    t.setName(namePrefix + threadNumber.getAndIncrement());
                                    if (t.isDaemon()) {
                                        t.setDaemon(false);
                                    }
                                    if (t.getPriority() != Thread.NORM_PRIORITY) {
                                        t.setPriority(Thread.NORM_PRIORITY);
                                    }
                                    return t;
                                }
                            });
                    executors.put(communicationType, service);
                }
            }
        }
        return service;
    }

    public int getAvailableThreads(CommunicationType communicationType) {
        ThreadPoolExecutor service = getExecutor(communicationType);
        return service.getMaximumPoolSize() - service.getActiveCount();
    }
    
    protected Date getLockTimeoutDate(CommunicationType communicationType) {
        String parameter = "";
        switch (communicationType) {
            case PULL:
                parameter = ParameterConstants.PULL_LOCK_TIMEOUT_MS;
                break;
            case PUSH:
                parameter = ParameterConstants.PUSH_LOCK_TIMEOUT_MS;
                break;
            case OFFLN_PULL:
                parameter = ParameterConstants.OFFLINE_PULL_LOCK_TIMEOUT_MS;
                break;
            case OFFLN_PUSH:
                parameter = ParameterConstants.OFFLINE_PUSH_LOCK_TIMEOUT_MS;
                break;
            case FILE_PULL:
                parameter = ParameterConstants.FILE_PULL_LOCK_TIMEOUT_MS;
                break;
            case FILE_PUSH:
                parameter = ParameterConstants.FILE_PUSH_LOCK_TIMEOUT_MS;
                break;
            case EXTRACT:
                parameter = ParameterConstants.INITIAL_LOAD_EXTRACT_TIMEOUT_MS;
                break;
            default:
                break;
        }
        return DateUtils.addMilliseconds(new Date(), 
                -parameterService.getInt(parameter, 7200000));
    }

    public boolean execute(final NodeCommunication nodeCommunication, RemoteNodeStatuses statuses,
            final INodeCommunicationExecutor executor) {
        Date now = new Date();
        Date lockTimeout = getLockTimeoutDate(nodeCommunication.getCommunicationType());
        final Set<String> executing = this.currentlyExecuting.get(nodeCommunication.getCommunicationType());
        boolean locked = !executing.contains(nodeCommunication.getNodeId()) && 
                sqlTemplate.update(getSql("aquireLockSql"), clusterService.getServerId(), now, now,
                nodeCommunication.getNodeId(), nodeCommunication.getCommunicationType().name(),
                lockTimeout) == 1;
        if (locked) {
            executing.add(nodeCommunication.getNodeId());
            nodeCommunication.setLastLockTime(now);
            nodeCommunication.setLockingServerId(clusterService.getServerId());
            final RemoteNodeStatus status = statuses.add(nodeCommunication.getNodeId());
            Runnable r = new Runnable() {
                public void run() {
                    long ts = System.currentTimeMillis();
                    boolean failed = false;
                    try {
                        executor.execute(nodeCommunication, status);
                        failed = status.failed();
                    } catch (Throwable ex) {
                        failed = true;
                        log.error(String.format("Failed to execute %s for node %s",
                                nodeCommunication.getCommunicationType().name(),
                                nodeCommunication.getNodeId()), ex);
                    } finally {
                        unlock(nodeCommunication, status, failed, ts);
                        executing.remove(nodeCommunication.getNodeId());
                    }
                }
            };
            if (parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)) {
                r.run();
            } else {
                ThreadPoolExecutor service = getExecutor(nodeCommunication.getCommunicationType());
                service.execute(r);
            }     
        }
        return locked;
    }
    
    protected void unlock(NodeCommunication nodeCommunication,
            RemoteNodeStatus status, boolean failed, long ts) {
        boolean unlocked = false;
        int attempts = 1;
        do {
            try {
                long millis = System.currentTimeMillis() - ts;
                nodeCommunication.setLockTime(null);
                nodeCommunication.setLastLockMillis(millis);
                if (failed) {
                    nodeCommunication.setFailCount(nodeCommunication
                            .getFailCount() + 1);
                    nodeCommunication.setTotalFailCount(nodeCommunication
                            .getTotalFailCount() + 1);
                    nodeCommunication.setTotalFailMillis(nodeCommunication
                            .getTotalFailMillis() + millis);
                } else {
                    nodeCommunication.setSuccessCount(nodeCommunication
                            .getSuccessCount() + 1);
                    nodeCommunication.setTotalSuccessCount(nodeCommunication
                            .getTotalSuccessCount() + 1);
                    nodeCommunication.setTotalSuccessMillis(nodeCommunication
                            .getTotalSuccessMillis() + millis);
                    nodeCommunication.setFailCount(0);
                }
                status.setComplete(true);
                save(nodeCommunication);
                unlocked = true;
                if (attempts > 1) {
                    log.info(String.format("Successfully unlocked %s node communication record for %s after %d attempts", 
                            nodeCommunication.getCommunicationType().name(),
                        nodeCommunication.getNodeId(), attempts));
                }
            } catch (Throwable e) {
                log.error(String.format(
                        "Failed to unlock %s node communication record for %s",
                        nodeCommunication.getCommunicationType().name(),
                        nodeCommunication.getNodeId()), e);
                long sleepTime = DateUtils.MILLIS_PER_SECOND
                        * new RandomTimeSlot(nodeCommunication.getNodeId(), 30).getRandomValueSeededByExternalId();
                log.warn("Sleeping for {} ms before attempting to unlock the node communication record again", sleepTime);
                AppUtils.sleep(sleepTime);
                attempts++;
            };
        } while (!unlocked);
    }    

    public void stop() {
        Collection<CommunicationType> services = new HashSet<NodeCommunication.CommunicationType>(
                executors.keySet());
        for (CommunicationType communicationType : services) {
            try {
                ExecutorService service = executors.get(communicationType);
                service.shutdownNow();
            } finally {
                executors.remove(communicationType);
            }
        }

    }

    class NodeCommunicationMapper implements ISqlRowMapper<NodeCommunication> {
        public NodeCommunication mapRow(Row rs) {
            NodeCommunication nodeCommuncation = new NodeCommunication();
            nodeCommuncation.setCommunicationType(CommunicationType.valueOf(rs.getString(
                    "communication_type").toUpperCase()));
            nodeCommuncation.setNodeId(rs.getString("node_id"));
            nodeCommuncation.setLockTime(rs.getDateTime("lock_time"));
            nodeCommuncation.setLastLockMillis(rs.getLong("last_lock_millis"));
            nodeCommuncation.setLockingServerId(rs.getString("locking_server_id"));
            nodeCommuncation.setSuccessCount(rs.getLong("success_count"));
            nodeCommuncation.setTotalSuccessCount(rs.getLong("total_success_count"));
            nodeCommuncation.setTotalSuccessMillis(rs.getLong("total_success_millis"));
            nodeCommuncation.setFailCount(rs.getLong("fail_count"));
            nodeCommuncation.setTotalFailCount(rs.getLong("total_fail_count"));
            nodeCommuncation.setTotalFailMillis(rs.getLong("total_fail_millis"));
            nodeCommuncation.setLastLockTime(rs.getDateTime("last_lock_time"));
            return nodeCommuncation;
        }
    }

}
