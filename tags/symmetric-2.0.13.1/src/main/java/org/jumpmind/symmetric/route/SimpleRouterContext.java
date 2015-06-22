package org.jumpmind.symmetric.route;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.model.NodeChannel;
import org.springframework.jdbc.core.JdbcTemplate;

public class SimpleRouterContext implements IRouterContext {

    protected NodeChannel channel;
    protected JdbcTemplate jdbcTemplate;
    protected boolean encountedTransactionBoundary = false;
    protected Map<String, Object> contextCache = new HashMap<String, Object>();
    protected String nodeId;

    public SimpleRouterContext(String nodeId, JdbcTemplate jdbcTemplate, NodeChannel channel) {
        this.init(jdbcTemplate, channel, nodeId);
    }

    public SimpleRouterContext() {
    }

    protected void init(JdbcTemplate jdbcTemplate, NodeChannel channel, String nodeId) {
        this.channel = channel;
        this.jdbcTemplate = jdbcTemplate;
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeChannel getChannel() {
        return this.channel;
    }

    public Map<String, Object> getContextCache() {
        return this.contextCache;
    }

    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    public void setEncountedTransactionBoundary(boolean encountedTransactionBoundary) {
        this.encountedTransactionBoundary = encountedTransactionBoundary;
    }

    public boolean isEncountedTransactionBoundary() {
        return this.encountedTransactionBoundary;
    }

    private final String getStatKey(String name) {
        return String.format("Stat.%s", name);
    }

    synchronized public void incrementStat(long amount, String name) {
        final String KEY = getStatKey(name);
        Long val = (Long) contextCache.get(KEY);
        if (val == null) {
            val = 0l;
        }
        val += amount;
        contextCache.put(KEY, val);
    }

    public long getStat(String name) {
        final String KEY = getStatKey(name);
        Long val = (Long) contextCache.get(KEY);
        if (val == null) {
            val = 0l;
        }
        return val;
    }

    public void logStats(ILog log, int dataCount, long totalTimeInMs) {
        boolean infoLevel = totalTimeInMs > 30000;
        Set<String> keys = new HashSet<String>(contextCache.keySet());
        for (String key : keys) {
            if (key.startsWith("Stat.")) {
                String keyString = key.substring(key.indexOf(".") + 1);
                if (infoLevel) {
                    log.info("RouterStats", channel.getChannelId(), keyString, contextCache.get(key));                    
                } else {
                    log.debug("RouterStats", channel.getChannelId(), keyString, contextCache.get(key));                    
                }               
            }
        }
        
        if (infoLevel) {
            log.info("RouterTimeForChannel", totalTimeInMs, dataCount, channel.getChannelId());
        } else {
            log.debug("RouterTimeForChannel", totalTimeInMs,  dataCount, channel.getChannelId());
        }
    }
}