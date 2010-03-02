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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Router;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

/**
 * A data router that uses a lookup table to map data to nodes
 */
public class LookupTableDataRouter extends AbstractDataRouter implements IDataRouter {

    final static ILog log = LogFactory.getLog(LookupTableDataRouter.class);

    public final static String PARAM_TABLE = "TABLE";
    public final static String PARAM_MAPPING_COLUMN = "MAPPING_COLUMN";
    public final static String PARAM_KEY_COLUMN = "KEY_COLUMN";
    public final static String PARAM_EXTERNAL_ID_COLUMN = "EXTERNAL_ID_COLUMN";

    final static String EXPRESSION_KEY = String.format("%s.Expression.",
            LookupTableDataRouter.class.getName());

    final static String LOOKUP_TABLE_KEY = String.format("%s.Table.", LookupTableDataRouter.class
            .getName());

    private JdbcTemplate jdbcTemplate;

    public Collection<String> routeToNodes(IRouterContext routingContext,
            DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {

        Set<String> nodeIds = null;
        Router router = dataMetaData.getTriggerRouter().getRouter();
        Map<String, String> params = getParams(router, routingContext);
        Map<String, String> dataMap = getDataMap(dataMetaData);
        boolean validExpression = params.containsKey(PARAM_TABLE)
                && params.containsKey(PARAM_KEY_COLUMN) && params.containsKey(PARAM_MAPPING_COLUMN)
                && params.containsKey(PARAM_EXTERNAL_ID_COLUMN);
        if (validExpression) {
            Map<String, String> lookupTable = getLookupTable(params, router, routingContext);
            String column = params.get(PARAM_MAPPING_COLUMN);
            String keyData = dataMap.get(column);
            String externalId = lookupTable.get(keyData);
            if (!StringUtils.isBlank(externalId)) {
                for (Node node : nodes) {
                    if (node.getExternalId().equals(externalId)) {
                        nodeIds = addNodeId(node.getNodeId(), nodeIds, nodes);
                    }
                }
            }

        } else {
            log.warn("RouterIllegalLookupTableExpression", router.getRouterExpression());
        }

        return nodeIds;

    }

    /**
     * Cache parsed expressions in the context to minimize the amount of parsing
     * we have to do when we have lots of throughput.
     */
    @SuppressWarnings("unchecked")
    protected Map<String, String> getParams(Router router, IRouterContext routingContext) {
        final String KEY = EXPRESSION_KEY + router.getRouterId();
        Map<String, String> params = (Map<String, String>) routingContext.getContextCache()
                .get(KEY);
        if (params == null) {
            params = new HashMap<String, String>();
            routingContext.getContextCache().put(KEY, params);
            String routerExpression = router.getRouterExpression();
            if (!StringUtils.isBlank(routerExpression)) {
                String[] expTokens = routerExpression.split("\r\n|\r|\n");
                if (expTokens != null) {
                    for (String t : expTokens) {
                        if (!StringUtils.isBlank(t)) {
                            String[] tokens = t.split("=");
                            if (tokens.length >= 2) {
                                params.put(tokens[0], tokens[1]);
                            }
                        }
                    }
                }
            }
        }
        return params;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> getLookupTable(Map<String, String> params, Router router,
            IRouterContext routingContext) {
        final String CTX_CACHE_KEY = LOOKUP_TABLE_KEY + "." + params.get("TABLENAME");
        Map<String, String> lookupMap = (Map<String, String>) routingContext.getContextCache().get(
                CTX_CACHE_KEY);
        if (lookupMap == null) {
            final Map<String, String> fillMap = new HashMap<String, String>();
            jdbcTemplate.query(String.format("select %s, %s from %s", params.get(PARAM_KEY_COLUMN),
                    params.get(PARAM_EXTERNAL_ID_COLUMN), params.get(PARAM_TABLE)),
                    new RowCallbackHandler() {
                        public void processRow(ResultSet rs) throws SQLException {
                            fillMap.put(rs.getString(1), rs.getString(2));
                        }
                    });
            lookupMap = fillMap;
            routingContext.getContextCache().put(CTX_CACHE_KEY, lookupMap);
        }
        return lookupMap;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
