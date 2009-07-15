package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.ParameterizedSingleColumnRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

public class SubSelectDataRouter extends AbstractDataRouter {

    private String sql;
    
    private JdbcTemplate jdbcTemplate;
    
    private IDbDialect dbDialect;
    
    public Collection<String> routeToNodes(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        Trigger trigger = dataMetaData.getTrigger();
        String subSelect = trigger.getRouterExpression();
        Collection<String> nodeIds = null;
        if (!StringUtils.isBlank(subSelect)) {
            SimpleJdbcTemplate simpleTemplate = new SimpleJdbcTemplate(jdbcTemplate);
            Map<String, Object> sqlParams = getNewData(dataMetaData, dbDialect);
            sqlParams.put("NODE_GROUP_ID", trigger.getTargetGroupId());
            nodeIds = simpleTemplate.query(String.format("%s%s", sql, subSelect), new ParameterizedSingleColumnRowMapper<String>(), sqlParams);
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
    
    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}
