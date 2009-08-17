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
 * TODO Javadoc
 */
public class SubSelectDataRouter extends AbstractDataRouter {

    private String sql;
    
    private JdbcTemplate jdbcTemplate;
    
    private IDbDialect dbDialect;
    
    public Collection<String> routeToNodes(IRouterContext routingContext, DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        TriggerRouter trigger = dataMetaData.getTrigger();
        String subSelect = trigger.getRouter().getRouterExpression();
        Collection<String> nodeIds = null;
        if (!StringUtils.isBlank(subSelect)) {
            SimpleJdbcTemplate simpleTemplate = new SimpleJdbcTemplate(jdbcTemplate);
            Map<String, Object> sqlParams = getDataObjectMap(dataMetaData, dbDialect);
            sqlParams.put("NODE_GROUP_ID", trigger.getRouter().getTargetGroupId());
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
