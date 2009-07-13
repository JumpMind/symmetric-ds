package org.jumpmind.symmetric.route;

import java.sql.Types;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;

public class SubSelectDataRouter extends AbstractDataRouter {

    private String sql;
    
    private JdbcTemplate jdbcTemplate;
    
    @SuppressWarnings("unchecked")
    public Collection<String> routeToNodes(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        Trigger trigger = dataMetaData.getTrigger();
        String subSelect = trigger.getRoutingExpression();
        Collection<String> nodeIds = null;
        if (!StringUtils.isBlank(subSelect)) {
            nodeIds = jdbcTemplate.queryForList(String.format("%s%s", sql, subSelect), new Object[] { trigger.getTargetGroupId() }, new int[] {Types.INTEGER}, String.class);
        } else {
            nodeIds = toNodeIds(nodes);
        }
        return nodeIds;
    }
    
    public void setSql(String sql) {
        this.sql = sql;
    }
    
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
