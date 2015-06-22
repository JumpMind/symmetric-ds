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
package org.jumpmind.symmetric.map;

import java.util.Map;
import java.util.TreeMap;

import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

/**
 * A column filter that can add additional columns to a table that is being loaded
 * at the node where this column filter is configured. 
 */
public class AddColumnsFilter implements ITableColumnFilter, INodeGroupExtensionPoint {

    private String[] tables;

    private Map<String, Object> additionalColumns;
    
    private boolean autoRegister = true;
    
    private String[] nodeGroupIdsToApplyTo;

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }
    
    public boolean isAutoRegister() {
        return autoRegister;
    }
    
    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroupIdsToApplyTo;
    }

    public void setNodeGroupIdsToApplyTo(String[] nodeGroupIdsToApplyTo) {
        this.nodeGroupIdsToApplyTo = nodeGroupIdsToApplyTo;
    }
    
    public Map<String, Object> getAdditionalColumns() {
        return additionalColumns;
    }

    public void setAdditionalColumns(Map<String, Object> columns) {
        this.additionalColumns = new TreeMap<String, Object>(columns);
    }

    public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table,
            String[] columnNames) {
        if (additionalColumns != null) {
            String[] columnNamesPlus = new String[columnNames.length + additionalColumns.size()];
            for (int i = 0; i < columnNames.length; i++) {
               columnNamesPlus[i] = columnNames[i];
            }
            
            int i = columnNames.length;
            for (String extraCol : additionalColumns.keySet()) {
                columnNamesPlus[i++] = extraCol;
            }
            return columnNamesPlus;
        } else {
            return columnNames;
        }
    }

    public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table,
            Object[] columnValues) {
        if (additionalColumns != null) {
            Object[] columnValuesPlus = new Object[columnValues.length + additionalColumns.size()];
            for (int i = 0; i < columnValues.length; i++) {
               columnValuesPlus[i] = columnValues[i];
            }
            
            int i = columnValues.length;
            for (String extraCol : additionalColumns.keySet()) {
                Object extraValue = additionalColumns.get(extraCol);
                if (TokenConstants.EXTERNAL_ID.equals(extraValue)) {
                    extraValue = ctx.getNode() != null ? ctx.getNode().getExternalId() : null;
                } else if (TokenConstants.NODE_ID.equals(extraValue)) {
                    extraValue = ctx.getNodeId();
                } else if (TokenConstants.NODE_GROUP_ID.equals(extraValue)) {
                    extraValue = ctx.getNode() != null ? ctx.getNode().getNodeGroupId() : null;
                }
                columnValuesPlus[i++] = extraValue;
            }
            return columnValuesPlus;
        } else {
            return columnValues;
        }
    }       


}