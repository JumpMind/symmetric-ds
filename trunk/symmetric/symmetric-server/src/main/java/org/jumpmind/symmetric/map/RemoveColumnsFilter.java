/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.map;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

/**
 * This filter may be configured to prevent specific columns from being loaded
 * at a target database.
 */
public class RemoveColumnsFilter implements ITableColumnFilter, INodeGroupExtensionPoint {

    private String[] columnsToRemove;

    private String[] tables;

    private String[] nodeGroupsToApplyTo;

    final String cacheKey = getClass().getName() + ".columnIndex";

    private boolean autoRegister = true;

    public String[] getTables() {
        return tables;
    }

    public void setColumnToRemove(String column) {
        this.columnsToRemove = new String[] { column };
    }

    public void setColumnsToRemove(String[] columnsToRemove) {
        this.columnsToRemove = columnsToRemove;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public void setTable(String table) {
        this.tables = new String[] { table };
    }

    public void setNodeGroupsToApplyTo(String[] nodeGroupsToApplyTo) {
        this.nodeGroupsToApplyTo = nodeGroupsToApplyTo;
    }

    public void setNodeGroupToApplyTo(String nodeGroupToApplyTo) {
        this.nodeGroupsToApplyTo = new String[] { nodeGroupToApplyTo };
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroupsToApplyTo;
    }

    @SuppressWarnings("unchecked")
    public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table,
            String[] columnNames) {
        if (dml != DmlType.DELETE) {
            Map<DmlType, List<Integer>> columnIndex = (Map<DmlType, List<Integer>>) ctx
                    .getContextCache().get(cacheKey);
            if (columnIndex == null) {
                columnIndex = new HashMap<DmlType, List<Integer>>(4);
                ctx.getContextCache().put(cacheKey, columnIndex);
            }
            List<String> columns = new ArrayList<String>();
            int index = 0;
            for (String col : columnNames) {
                if (ArrayUtils.contains(columnsToRemove, col)) {
                    Integer putValue = index;
                    if (columnIndex.get(dml) == null) {
                        columnIndex.put(dml, new ArrayList<Integer>());
                    }
                    columnIndex.get(dml).add(putValue);
                } else {
                    columns.add(col);
                }
                index++;
            }

            return (String[]) columns.toArray(new String[columns.size()]);
        } else {
            return columnNames;
        }
    }

    @SuppressWarnings("unchecked")
    public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table,
            Object[] columnValues) {
        if (dml != DmlType.DELETE) {
            Map<DmlType, List<Integer>> columnIndex = (Map<DmlType, List<Integer>>) ctx
                    .getContextCache().get(cacheKey);
            if (columnIndex != null) {
                List<Integer> indexes = columnIndex.get(dml);
                if (indexes != null) {
                    Object[] sourceValues = columnValues;
                    columnValues = new Object[columnValues.length - indexes.size()];
                    int targetIndex = 0;
                    int sourceIndex = 0;
                    for (Object sourceValue : sourceValues) {
                        if (!indexes.contains(sourceIndex)) {
                            columnValues[targetIndex++] = sourceValue;
                        }
                        sourceIndex++;
                    }
                }
            }
        }

        return columnValues;

    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

}
