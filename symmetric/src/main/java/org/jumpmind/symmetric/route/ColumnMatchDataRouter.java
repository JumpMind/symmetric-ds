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
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter {

    private IDbDialect dbDialect;

    public Collection<String> routeToNodes(DataMetaData dataMetaData, Set<Node> nodes, boolean initialLoad) {
        Collection<String> nodeIds = null;
        String expression = dataMetaData.getTrigger().getRouterExpression();
        if (!StringUtils.isBlank(expression)) {
            Map<String, String> columnValues = getNewDataAsString(dataMetaData, dbDialect);
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

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
}
