package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;

public class NodeCommunicationService extends AbstractService implements INodeCommunicationService {

    private String serverId = AppUtils.getServerId();

    private Map<CommunicationType, ThreadPoolExecutor> executors = new HashMap<NodeCommunication.CommunicationType, ThreadPoolExecutor>();

    private INodeService nodeService;

    private boolean initialized = false;

    public NodeCommunicationService(INodeService nodeService, IParameterService parameterService,
            ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
        setSqlMap(new NodeCommunicationServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
        this.nodeService = nodeService;
    }

    private final void initialize() {
        if (!initialized) {
            try {
                int locksCleared = sqlTemplate.update(getSql("clearLocksOnRestartSql"), serverId);
                if (locksCleared > 0) {
                    log.info("Cleared {} node communication locks for {}", locksCleared, serverId);
                }
            } finally {
                initialized = true;
            }
        }
    }

    public List<NodeCommunication> list(CommunicationType communicationType) {
        initialize();
        List<NodeCommunication> communicationRows = new ArrayList<NodeCommunication>(
                sqlTemplate.query(getSql("selectNodeCommunicationSql"),
                        new NodeCommunicationMapper(), communicationType.name()));
        List<Node> nodesToCommunicateWith = null;
        switch (communicationType) {
            case PULL:
                nodesToCommunicateWith = nodeService.findNodesToPull();
                break;
            case PUSH:
                nodesToCommunicateWith = nodeService.findNodesToPushTo();
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

        for (NodeCommunication nodeCommunication : communicationRows) {
            Node node = null;
            for (Node nodeToCommunicateWith : nodesToCommunicateWith) {
                if (nodeCommunication.getNodeId().equals(nodeToCommunicateWith.getNodeId())) {
                    node = nodeToCommunicateWith;
                    break;
                }
            }

            if (node == null) {
                delete(nodeCommunication);
                communicationRows.remove(nodeCommunication);
            }
        }
        return communicationRows;
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
        if (service == null) {
            synchronized (this) {
                service = executors.get(communicationType);
                if (service == null) {
                    String threadCountParameter = "";
                    switch (communicationType) {
                        case PULL:
                            threadCountParameter = ParameterConstants.PULL_THREAD_COUNT_PER_SERVER;
                            break;
                        case PUSH:
                            threadCountParameter = ParameterConstants.PUSH_THREAD_COUNT_PER_SERVER;
                            break;
                    }
                    int threadCount = parameterService.getInt(threadCountParameter, 1);

                    if (threadCount <= 0) {
                        log.warn("{}={} is not a legal value. Defaulting to 1",
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
        }
        return DateUtils.add(new Date(), Calendar.MILLISECOND,
                -parameterService.getInt(parameter, 7200000));
    }

    public boolean execute(final NodeCommunication nodeCommunication, RemoteNodeStatuses statuses,
            final INodeCommunicationExecutor executor) {
        Date now = new Date();
        Date lockTimeout = getLockTimeoutDate(nodeCommunication.getCommunicationType());
        boolean locked = sqlTemplate.update(getSql("aquireLockSql"), serverId, now, now,
                nodeCommunication.getNodeId(), nodeCommunication.getCommunicationType().name(),
                lockTimeout, serverId) == 1;
        if (locked) {
            nodeCommunication.setLastLockTime(now);
            nodeCommunication.setLockingServerId(serverId);
            final RemoteNodeStatus status = statuses.add(nodeCommunication.getNode());
            ThreadPoolExecutor service = getExecutor(nodeCommunication.getCommunicationType());
            service.execute(new Runnable() {
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
                        long millis = System.currentTimeMillis() - ts;
                        nodeCommunication.setLockTime(null);
                        nodeCommunication.setLastLockMillis(millis);
                        if (failed) {
                            nodeCommunication.setFailCount(nodeCommunication.getFailCount() + 1);
                            nodeCommunication.setTotalFailCount(nodeCommunication
                                    .getTotalFailCount() + 1);
                            nodeCommunication.setTotalFailMillis(nodeCommunication
                                    .getTotalFailMillis() + millis);
                        } else {
                            nodeCommunication.setSuccessCount(nodeCommunication.getSuccessCount() + 1);
                            nodeCommunication.setTotalSuccessCount(nodeCommunication
                                    .getTotalSuccessCount() + 1);
                            nodeCommunication.setTotalSuccessMillis(nodeCommunication
                                    .getTotalSuccessMillis() + millis);
                        }
                        status.setComplete(true);
                        save(nodeCommunication);
                    }
                }
            });
        }
        return locked;
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
