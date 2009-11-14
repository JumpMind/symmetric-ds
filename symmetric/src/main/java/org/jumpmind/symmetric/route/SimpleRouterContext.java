package org.jumpmind.symmetric.route;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.NodeChannel;
import org.springframework.jdbc.core.JdbcTemplate;

public class SimpleRouterContext implements IRouterContext {

    protected final ILog log = LogFactory.getLog(getClass());
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

    public void incrementStat(long amount, String name) {
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

    public void logStats(ILog log) {
        Set<String> keys = contextCache.keySet();
        for (String key : keys) {
            if (key.startsWith("Stat.")) {
                log.debug("RouterStats", channel.getChannelId(), key.substring(key.indexOf(".") + 1), contextCache.get(key));
            }
        }
    }
}
