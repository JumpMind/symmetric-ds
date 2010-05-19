/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) JumpMind, Inc
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

package org.jumpmind.symmetric.db;

import java.util.ArrayList;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

public class AutoIncrementColumnFilter implements IColumnFilter {

    int[] indexesToRemove = null;

    public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table,
            String[] columnNames) {
        indexesToRemove = null;
        if (dml == DmlType.UPDATE) {
            Column[] autoIncrementColumns = table.getAutoIncrementColumns();
            if (autoIncrementColumns != null && autoIncrementColumns.length > 0) {
                ArrayList<String> columns = new ArrayList<String>();
                CollectionUtils.addAll(columns, columnNames);
                indexesToRemove = new int[autoIncrementColumns.length];
                int i = 0;
                for (Column column : autoIncrementColumns) {
                    String name = column.getName();
                    int index = columns.indexOf(name);

                    if (index < 0) {
                        name = name.toLowerCase();
                        index = columns.indexOf(name);
                    }
                    if (index < 0) {
                        name = name.toUpperCase();
                        index = columns.indexOf(name);
                    }
                    
                    indexesToRemove[i++] = index;
                    columns.remove(name);
                }
                columnNames = columns.toArray(new String[columns.size()]);
            }
        }
        return columnNames;

    }

    public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table,
            Object[] columnValues) {
        if (dml == DmlType.UPDATE && indexesToRemove != null) {
            ArrayList<Object> values = new ArrayList<Object>();
            CollectionUtils.addAll(values, columnValues);
            for (int index : indexesToRemove) {
                if (index >= 0) {
                    values.remove(index);
                }
            }
            return values.toArray(new Object[values.size()]);
        }
        return columnValues;
    }

    public boolean isAutoRegister() {
        return false;
    }
}
