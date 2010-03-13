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

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.springframework.scripting.ScriptCompilationException;

public class ColumnDataFilters implements IDataLoaderFilter, INodeGroupExtensionPoint {

    final ILog log = LogFactory.getLog(getClass());

    private boolean autoRegister = true;

    private String[] nodeGroupIdsToApplyTo;

    List<TableColumnValueFilter> filters;

    private boolean ignoreCase = true;

    protected void filterColumnValues(IDataLoaderContext context, String[] columnValues) {
        if (filters != null) {
            for (TableColumnValueFilter filteredColumn : filters) {
                if ((ignoreCase && filteredColumn.getTableName().equalsIgnoreCase(
                        context.getTableName()))
                        || (!ignoreCase && filteredColumn.getTableName().equals(
                                context.getTableName()))) {
                    String columnName = filteredColumn.getColumnName();
                    int index = context.getColumnIndex(columnName);
                    if (index < 0 && ignoreCase) {
                        columnName = columnName.toUpperCase();
                        index = context.getColumnIndex(columnName);
                        if (index < 0) {
                            columnName = columnName.toLowerCase();
                            index = context.getColumnIndex(columnName);
                        }
                    }
                    if (index >= 0) {
                        try {
                            columnValues[index] = filteredColumn.getFilter().filter(
                                    columnValues[index], context.getContextCache());
                        } catch (RuntimeException ex) {
                            // Try to log script errors so they are more readable
                            Throwable causedBy = ex;
                            do {
                                causedBy = ExceptionUtils.getCause(causedBy);
                                if (causedBy instanceof ScriptCompilationException) {
                                    log.error("Message", causedBy.getMessage());
                                    throw new RuntimeException(causedBy.getMessage());
                                }
                            } while (causedBy != null);
                            throw ex;
                        }
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

    public void setFilters(List<TableColumnValueFilter> filters) {
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
    
    public void setNodeGroupIdToApplyTo(String nodeGroupId) {
        this.nodeGroupIdsToApplyTo = new String[] { nodeGroupId };
    }

    public void setIgnoreCase(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }
}
