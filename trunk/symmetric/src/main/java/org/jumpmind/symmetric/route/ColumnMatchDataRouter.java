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
