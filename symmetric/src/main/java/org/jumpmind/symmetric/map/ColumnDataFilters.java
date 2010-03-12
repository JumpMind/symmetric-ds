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
import java.util.Set;

import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;

public class ColumnDataFilters implements IDataLoaderFilter, INodeGroupExtensionPoint {

    private boolean autoRegister = true;

    private String[] nodeGroupIdsToApplyTo;

    Map<String, Map<String, IColumnFilter>> filters;

    protected void filterColumnValues(IDataLoaderContext context, String[] columnValues) {
        Map<String, IColumnFilter> filteredColumns = filters.get(context.getTableName());
        if (filteredColumns != null) {
            Set<String> columns = filteredColumns.keySet();
            if (columns != null) {
                for (String column : columns) {
                    int index = context.getColumnIndex(column);
                    if (index >= 0) {
                        columnValues[index] = filteredColumns.get(column).filter(
                                columnValues[index], context.getContextCache());
                    }
                }
            }
        }
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        filterColumnValues(context, columnValues);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues,
            String[] keyValues) {
        filterColumnValues(context, columnValues);
        return true;
    }

    public void setFilters(Map<String, Map<String, IColumnFilter>> filters) {
        this.filters = filters;
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return this.nodeGroupIdsToApplyTo;
    }

    public void setNodeGroupIdsToApplyTo(String[] nodeGroupIdsToApplyTo) {
        this.nodeGroupIdsToApplyTo = nodeGroupIdsToApplyTo;
    }
}
