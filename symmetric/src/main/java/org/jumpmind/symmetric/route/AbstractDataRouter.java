package org.jumpmind.symmetric.route;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.ddlutils.model.Column;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

public abstract class AbstractDataRouter implements IDataRouter {

    private boolean autoRegister = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }
    
    protected Map<String, String> getNewDataAsString(DataMetaData dataMetaData, IDbDialect dbDialect) {
        String[] rowData = dataMetaData.getData().getParsedRowData();
        Column[] columns = dataMetaData.getTable().getColumns();
        Map<String, String> map = new HashMap<String, String>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            map.put(c.getName(), rowData[i]);
        }
        return map;        
    }
  
    protected Map<String, Object> getNewData(DataMetaData dataMetaData, IDbDialect dbDialect) {
        String[] rowData = dataMetaData.getData().getParsedRowData();
        Column[] columns = dataMetaData.getTable().getColumns();
        Map<String, Object> map = new HashMap<String, Object>(columns.length);
        Object[] objects = dbDialect.getObjectValues(dbDialect.getBinaryEncoding(), rowData, columns);
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            map.put(c.getName(), objects[i]);
        }
        return map;        
    }

    protected Set<String> toNodeIds(Set<Node> nodes) {
        Set<String> nodeIds = new HashSet<String>(nodes.size());
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }
    

}
