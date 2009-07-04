package org.jumpmind.symmetric.route;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.Trigger;

public class ColumnMatchDataRouter extends AbstractDataRouter implements IDataRouter {

    protected String columnName;

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Set<String> routeToNodes(Data data, Trigger trigger, List<org.jumpmind.symmetric.model.Node> nodes,
            NodeChannel channel) {
        return null;
    }

}
