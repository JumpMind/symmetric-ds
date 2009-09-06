/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.ParameterizedSingleColumnRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

/**
 * This data router is invoked when the router_name is 'subselect'. The router_expression is always a SQL expression
 * that is used to find the nodes a row of data will be routed to. This router should never be used for high throughput
 * tables because it makes a call back to the database for each row that is synchronized.
 * <P/>
 * The query that is used to select the nodes is as follows:
 * <P/>
 * <code>
 * select c.node_id from $[sym.sync.table.prefix]_node c where c.node_group_id=:NODE_GROUP_ID and c.sync_enabled=1 and ...
 * </code>
 * <P/>
 * The SQL expression designated in the router_expression is appended to the SQL statement.  
 */
public class SubSelectDataRouter extends AbstractDataRouter {

    private String sql;

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    public Collection<String> routeToNodes(IRouterContext routingContext, DataMetaData dataMetaData, Set<Node> nodes,
            boolean initialLoad) {
        TriggerRouter trigger = dataMetaData.getTrigger();
        String subSelect = trigger.getRouter().getRouterExpression();
        Collection<String> nodeIds = null;
        if (!StringUtils.isBlank(subSelect)) {
            SimpleJdbcTemplate simpleTemplate = new SimpleJdbcTemplate(jdbcTemplate);
            Map<String, Object> sqlParams = getDataObjectMap(dataMetaData, dbDialect);
            sqlParams.put("NODE_GROUP_ID", trigger.getRouter().getTargetNodeGroupId());
            nodeIds = simpleTemplate.query(String.format("%s%s", sql, subSelect),
                    new ParameterizedSingleColumnRowMapper<String>(), sqlParams);
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
