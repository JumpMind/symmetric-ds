package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.job.ping.*;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.sql.Types;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.jumpmind.symmetric.job.JobDefaults.EVERY_30_SECONDS;


public class NodeOnlineDetectorJob extends AbstractJob {

    final Logger log = LoggerFactory.getLogger(getClass());
    private ConcurrentHashMap<String, NodeOnlineStatus> oldPingResults;
    private ExecutorService executor;
    private ISqlTemplate sqlTemplate;

    public NodeOnlineDetectorJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super(ClusterConstants.NODE_ONLINE_DETECTOR, engine, taskScheduler);
        this.oldPingResults = new ConcurrentHashMap<>();
        this.executor = Executors.newCachedThreadPool();
        this.sqlTemplate = engine.getSqlTemplate();
    }

    @Override
    public void doJob(boolean force){
        synchronized (this.getClass()) {
            pingNodes();
        }
    }

    @Override
    public JobDefaults getDefaults() {
        return new JobDefaults()
                .schedule(EVERY_30_SECONDS)
                .enabled(false)
                .description("Ping all nodes");
    }

    public ConcurrentHashMap<String, NodeOnlineStatus.PossibleStatus> pingNodes() {
        List<Future<PingResult>> list = new ArrayList<Future<PingResult>>();
        for(Map.Entry<String, String> entry : getHostnames().entrySet()) {
            String nodeId = entry.getKey();
            String host = entry.getValue();
            Callable<PingResult> callable = new PingTask(nodeId, host);
            Future<PingResult> future = executor.submit(callable);
            list.add(future);
        }
        ConcurrentHashMap<String, NodeOnlineStatus.PossibleStatus> currentPingResults = new ConcurrentHashMap<>();
        try {
            for(Future<PingResult> fut : list){
                try {
                    PingResult result = fut.get();
                    currentPingResults.put(result.getNodeId(),result.getResultingStatus());
                } catch (Exception e) {
                    log.error("", e);
                }
            }
            memorizePingResults(currentPingResults);
            //logSomeStatistic(currentPingResults);
        } catch (Exception e) {
            log.error("", e);
        }

        return currentPingResults;
    }

    private void memorizePingResults(ConcurrentHashMap<String, NodeOnlineStatus.PossibleStatus> currentPingResults) {
        for(Map.Entry<String, NodeOnlineStatus.PossibleStatus> node : currentPingResults.entrySet()) {
            String nodeId = node.getKey();
            NodeOnlineStatus.PossibleStatus currentStatus = node.getValue();
            if(! oldPingResults.containsKey(nodeId)) {
                oldPingResults.put(nodeId, new NodeOnlineStatus(currentStatus));
                if(currentStatus == NodeOnlineStatus.PossibleStatus.Online) {
                    oldPingResults.get(nodeId).setWentOnline(new Date());
                }
            } else if(oldPingResults.get(nodeId).getStatus() != currentStatus) {
                if(currentStatus == NodeOnlineStatus.PossibleStatus.Online) {
                    oldPingResults.get(nodeId).setWentOnline(new Date());
                } else if(oldPingResults.get(nodeId).getStatus() == NodeOnlineStatus.PossibleStatus.Online && currentPingResults.get(nodeId) == NodeOnlineStatus.PossibleStatus.Offline) {
                    if(engine.getParameterService().is(ParameterConstants.STORE_NODE_ONLINE_HISTORY, false)) {
                        insertIntoHistoryTable(nodeId, oldPingResults.get(nodeId).getWentOnline(), new Date());
                    }
                    oldPingResults.get(nodeId).setWentOnline(null);
                }
                oldPingResults.get(nodeId).setStatus(currentStatus);
            }
        }
    }

    private void insertIntoHistoryTable(String nodeId, Date wentOnlineTime, Date wentOfflineTime) {
        try {
            sqlTemplate.update("insert into sym_node_online_history (node_id, went_online_time, went_offline_time) values(?,?,?);",
                    new Object[] {
                            nodeId,
                            wentOnlineTime,
                            wentOfflineTime
                    }, new int[] {
                            Types.VARCHAR,
                            Types.TIMESTAMP,
                            Types.TIMESTAMP
                    } );
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void logSomeStatistic(ConcurrentHashMap<String, NodeOnlineStatus.PossibleStatus> currentPingResults) {
        try {
            Map<NodeOnlineStatus.PossibleStatus, Long> result = currentPingResults.values().stream().collect(Collectors.groupingBy(Function.identity(),Collectors.counting()));
            log.info(result.toString());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private HashMap<String, String> getHostnames() {
        List<Node> nodes = engine.getNodeService().findAllNodes();
        List<Row> hostsRows = sqlTemplate.query("select node_id, host_name from sym_node_host");

        HashMap<String, String> nodeHosts = new HashMap<>();
        hostsRows.forEach(x ->
                nodeHosts.put(x.getString("node_id"), x.getString("host_name"))
        );

        HashMap<String, String> res = new HashMap<>();
        nodes.forEach( x -> {
            String hostname = nodeHosts.containsKey(x.getNodeId()) ? nodeHosts.get(x.getNodeId()) : x.getExternalId();
            res.put(x.getNodeId(), hostname);
        });
        return res;
    }

}
