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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IRegistrationService;

/**
 * This data router is invoked when the router_name='column'. The router_expression is always a name value pair of a
 * column on the table that is being synchronized to the value it should be matched with.
 * <P>
 * The value can be a constant. In the data router the value of the new data is always represented by a string so all
 * comparisons are done in the format that SymmetricDS transmits.
 * <P>
 * The column name used for the match is the upper case column name if the current value is being compared.  The upper case column name
 * prefixed by OLD_ can be used if the comparison is being done of the old data.
 * <P>
 * For example, if the column on a table is named STATUS you can specify that you want to router when STATUS=OK by specifying such for the router_expression.  If you wanted to route when
 * only the old value for STATUS=OK you would specify OLD_STATUS=OK.
 * <P>
 * The value can also be one of the following expressions:
 * <ol>
 * <li>:NODE_ID</li>
 * <li>:EXTERNAL_ID</li>
 * <li>:NODE_GROUP_ID</li>
 * <li>:REDIRECT_NODE</li>
 * </ol>
 * NODE_ID, EXTERNAL_ID, and NODE_GROUP_ID are instructions for the column matcher to select nodes that have a NODE_ID,
 * EXTERNAL_ID or NODE_GROUP_ID that are equal to the value on the column.
 * <P>
 * REDIRECT_NODE is an instruction to match the specified column to a registrant_external_id on registration_redirect
 * and return the associated registration_node_id in the list of node id to route to. For example, if the 'price' table
 * was being routed to to a region 1 node based on the store_id, the store_id would be the external_id of a node in the
 * registration_redirect table and the router_expression for trigger entry for the 'price' table would be
 * 'store_id=:REDIRECT_NODE' and the router_name would be 'column'.
 * 
 */
public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter {
    private IRegistrationService registrationService;

    public Collection<String> routeToNodes(IRouterContext routingContext, DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        Collection<String> nodeIds = null;
        String expression = dataMetaData.getTrigger().getRouter().getRouterExpression();
        if (!StringUtils.isBlank(expression)) {
            Map<String, String> columnValues = getDataMap(dataMetaData);
            String[] tokens = expression.split("=");
            String column = tokens[0];
            String value = tokens[1];
            if (value.equalsIgnoreCase(":NODE_ID")) {
                nodeIds = new HashSet<String>();
                for (Node node : nodes) {
                    if (node.getNodeId().equals(columnValues.get(column))) {
                        nodeIds.add(node.getNodeId());
                    }
                }
            } else if (value.equalsIgnoreCase(":EXTERNAL_ID")) {
                nodeIds = new HashSet<String>();
                for (Node node : nodes) {
                    if (node.getExternalId().equals(columnValues.get(column))) {
                        nodeIds.add(node.getNodeId());
                    }
                }
            } else if (value.equalsIgnoreCase(":NODE_GROUP_ID")) {
                nodeIds = new HashSet<String>();
                for (Node node : nodes) {
                    if (node.getNodeGroupId().equals(columnValues.get(column))) {
                        nodeIds.add(node.getNodeId());
                    }
                }
            } else if (value.equalsIgnoreCase(":REDIRECT_NODE")) {
                nodeIds = new HashSet<String>();
                // TODO should we do any caching here? I am starting to lose track of where all we cache. Maybe we need
                // a pattern or central service or manager for caching??
                Map<String, String> redirectMap = registrationService.getRegistrationRedirectMap();
                String nodeId = redirectMap.get(columnValues.get(column));
                if (nodeId != null) {
                    nodeIds.add(nodeId);
                }
            } else {
                if (value.equals(columnValues.get(column))) {
                    nodeIds = toNodeIds(nodes);
                }
            }

        } else {
            nodeIds = toNodeIds(nodes);
        }

        return nodeIds;

    }
    
    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }
}
