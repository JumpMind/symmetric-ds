package org.jumpmind.symmetric.route;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.NodeChannel;
import org.springframework.jdbc.core.JdbcTemplate;

public class SimpleRouterContext implements IRouterContext {

    protected final Log logger = LogFactory.getLog(getClass());
    protected NodeChannel channel;
    protected JdbcTemplate jdbcTemplate;
    protected boolean encountedTransactionBoundary = false;
    protected Map<String, Object> contextCache = new HashMap<String, Object>();

    public SimpleRouterContext(JdbcTemplate jdbcTemplate, NodeChannel channel) {
        this.init(jdbcTemplate, channel);
    }

    public SimpleRouterContext() {
    }

    protected void init(JdbcTemplate jdbcTemplate, NodeChannel channel) {
        this.channel = channel;
        this.jdbcTemplate = jdbcTemplate;
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

}
