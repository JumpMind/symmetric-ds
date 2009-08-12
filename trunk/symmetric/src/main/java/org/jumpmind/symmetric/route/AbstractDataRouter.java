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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Column;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

public abstract class AbstractDataRouter implements IDataRouter {

    protected final Log logger = LogFactory.getLog(getClass());
    
    private boolean autoRegister = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }
    
    protected Map<String, String> getNewDataAsString(DataMetaData dataMetaData) {
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
        Map<String, Object> newData = new HashMap<String, Object>();
        String[] rowData = dataMetaData.getData().getParsedRowData();
        String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
        Object[] objects = dbDialect.getObjectValues(dbDialect.getBinaryEncoding(), dataMetaData.getTable(), columnNames, rowData);
        for (int i = 0; i < columnNames.length; i++) {
            newData.put(columnNames[i], objects[i]);
        }
        return newData;        
    }
    
    

    protected Set<String> toNodeIds(Set<Node> nodes) {
        Set<String> nodeIds = new HashSet<String>(nodes.size());
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }
    

}
