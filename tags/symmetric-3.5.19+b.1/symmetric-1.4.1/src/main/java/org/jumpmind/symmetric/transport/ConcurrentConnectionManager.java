package org.jumpmind.symmetric.transport;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticName;

public class ConcurrentConnectionManager implements IConcurrentConnectionManager {

    private IParameterService parameterService;

    private IStatisticManager statisticManager;

    protected Map<String, Map<String, Reservation>> activeReservationsByNodeByPool = new HashMap<String, Map<String, Reservation>>();

    protected Map<String, Map<String, NodeConnectionStatistics>> nodeConnectionStatistics = new HashMap<String, Map<String, NodeConnectionStatistics>>();

    protected Set<String> whiteList = new HashSet<String>();

    protected void logTooBusyRejection(String nodeId, String poolId) {
        getNodeConnectionStatistics(nodeId, poolId).numOfRejections++;
    }

    protected void logConnectedTimePeriod(String nodeId, long startMs, long endMs, String poolId) {
        NodeConnectionStatistics stats = getNodeConnectionStatistics(nodeId, poolId);
        stats.totalConnectionCount++;
        stats.totalConnectionTimeMs += endMs - startMs;
        stats.lastConnectionTimeMs = startMs;
    }

    private synchronized NodeConnectionStatistics getNodeConnectionStatistics(String nodeId, String poolId) {
        Map<String, NodeConnectionStatistics> statsMap = nodeConnectionStatistics.get(poolId);
        if (statsMap == null) {
            statsMap = new HashMap<String, NodeConnectionStatistics>();
            nodeConnectionStatistics.put(poolId, statsMap);
        }
        NodeConnectionStatistics stats = statsMap.get(nodeId);
        if (stats == null) {
            stats = new NodeConnectionStatistics();
            statsMap.put(nodeId, stats);
        }
        return stats;
    }

    synchronized public boolean releaseConnection(String nodeId, String poolId) {
        Map<String, Reservation> reservations = getReservationMap(poolId);
        Reservation reservation = reservations.remove(nodeId);
        if (reservation != null) {
            logConnectedTimePeriod(nodeId, reservation.createTime, System.currentTimeMillis(), poolId);
            return true;
        } else {
            return false;
        }
    }

    synchronized public void addToWhitelist(String nodeId) {
        whiteList.add(nodeId);
    }

    synchronized public void removeFromWhiteList(String nodeId) {
        whiteList.remove(nodeId);
    }

    synchronized public String[] getWhiteList() {
        return whiteList.toArray(new String[whiteList.size()]);
    }

    public int getReservationCount(String poolId) {
        return getReservationMap(poolId).size();
    }

    synchronized public boolean reserveConnection(String nodeId, String poolId, ReservationType reservationRequest) {
        Map<String, Reservation> reservations = getReservationMap(poolId);
        int maxPoolSize = parameterService.getInt(ParameterConstants.CONCURRENT_WORKERS);
        long timeout = parameterService.getLong(ParameterConstants.CONCURRENT_RESERVATION_TIMEOUT);
        removeTimedOutReservations(reservations, timeout);
        if (reservations.size() < maxPoolSize || reservations.containsKey(nodeId) || whiteList.contains(nodeId)) {
            reservations.put(nodeId, new Reservation(nodeId, reservationRequest == ReservationType.SOFT ? System
                    .currentTimeMillis()
                    + timeout : Long.MAX_VALUE));
            statisticManager.getStatistic(
                    reservationRequest == ReservationType.HARD ? StatisticName.NODE_CONCURRENCY_RESERVATION_REQUESTED
                            : StatisticName.NODE_CONCURRENCY_CONNECTION_RESERVED).increment();
            return true;
        } else {
            statisticManager.getStatistic(StatisticName.NODE_CONCURRENCY_TOO_BUSY_COUNT).increment();
            return false;
        }
    }

    private void removeTimedOutReservations(Map<String, Reservation> reservations, long timeout) {
        long currentTime = System.currentTimeMillis();
        for (Iterator<String> iterator = reservations.keySet().iterator(); iterator.hasNext();) {
            String nodeId = iterator.next();
            Reservation reservation = reservations.get(nodeId);
            if (reservation.timeToLiveInMs < currentTime) {
                statisticManager.getStatistic(StatisticName.NODE_CONCURRENCY_RESERVATION_TIMEOUT_COUNT).increment();
                reservations.remove(nodeId);
            }
        }
    }

    private Map<String, Reservation> getReservationMap(String poolId) {
        Map<String, Reservation> reservations = activeReservationsByNodeByPool.get(poolId);
        if (reservations == null) {
            reservations = new HashMap<String, Reservation>();
            activeReservationsByNodeByPool.put(poolId, reservations);
        }
        return reservations;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    public class Reservation {
        String nodeId;
        long timeToLiveInMs;
        long createTime = System.currentTimeMillis();

        public Reservation(String nodeId, long timeToLiveInMs) {
            this.nodeId = nodeId;
            this.timeToLiveInMs = timeToLiveInMs;
        }

        @Override
        public int hashCode() {
            return nodeId.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Reservation) {
                return nodeId.equals(((Reservation) obj).nodeId);
            } else {
                return false;
            }

        }

        public String getNodeId() {
            return nodeId;
        }

        public long getTimeToLiveInMs() {
            return timeToLiveInMs;
        }

        public long getCreateTime() {
            return createTime;
        }
    }

    public Map<String, Map<String, NodeConnectionStatistics>> getNodeConnectionStatisticsByPoolByNodeId() {
        return this.nodeConnectionStatistics;
    }

    public class NodeConnectionStatistics {

        int numOfRejections;
        long totalConnectionCount;
        long totalConnectionTimeMs;
        long lastConnectionTimeMs;

        public int getNumOfRejections() {
            return numOfRejections;
        }

        public long getTotalConnectionCount() {
            return totalConnectionCount;
        }

        public long getTotalConnectionTimeMs() {
            return totalConnectionTimeMs;
        }

        public long getLastConnectionTimeMs() {
            return lastConnectionTimeMs;
        }
    }

    public Map<String, Map<String, Reservation>> getActiveReservationsByNodeByPool() {
        return activeReservationsByNodeByPool;
    }

}
