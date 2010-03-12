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
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

public class ChangeColumnsNamesFilter implements ITableColumnFilter, INodeGroupExtensionPoint {

    private String[] tables;

    private Map<String, String> columnNameMapping;

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

    public Map<String, String> getColumnNameMapping() {
        return columnNameMapping;
    }

    public void setColumnNameMapping(Map<String, String> columns) {
        this.columnNameMapping = new TreeMap<String, String>(columns);
    }

    public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table,
            String[] columnNames) {
        if (columnNameMapping != null) {
            for (int i = 0; i < columnNames.length; i++) {
                String newColumnName = columnNameMapping.get(columnNames[i]);
                if (newColumnName != null) {
                    columnNames[i] = newColumnName;
                }
            }
        }
        return columnNames;
    }

    public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table,
            Object[] columnValues) {
        return columnValues;
    }

}