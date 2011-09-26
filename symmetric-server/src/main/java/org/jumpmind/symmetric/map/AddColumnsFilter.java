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
 * under the License.  */

package org.jumpmind.symmetric.map;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.ITableColumnFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;

/**
 * A column filter that can add additional columns to a table that is being loaded
 * at the node where this column filter is configured. 
 */
public class AddColumnsFilter implements ITableColumnFilter, INodeGroupExtensionPoint {

    private static final ILog log = LogFactory.getLog(AddColumnsFilter.class);
    
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
                    extraValue = ctx.getSourceNode() != null ? ctx.getSourceNode().getExternalId() : null;
                } else if (TokenConstants.NODE_ID.equals(extraValue)) {
                    extraValue = ctx.getSourceNode();
                } else if (TokenConstants.NODE_GROUP_ID.equals(extraValue)) {
                    extraValue = ctx.getSourceNode() != null ? ctx.getSourceNode().getNodeGroupId() : null;
                } else if (extraValue instanceof String && extraValue.toString().startsWith(":")){
                    String extraColumnName = extraValue.toString().substring(1);
                    int index = ctx.getFilteredColumnIndex(extraColumnName);
                    if (index >= 0) {
                        if (columnValues.length > index) {
                            extraValue = columnValues[index];
                        } else {
                            log.error(
                                    "Message",
                                    String.format(
                                            "The column name of %s was found, but the index, %d, was greater than the array of values.\n The column names were: %s\n The column values were: %s",
                                            extraColumnName, index, ArrayUtils.toString(ctx.getFilteredColumnNames()), ArrayUtils.toString(columnValues)));
                            ;
                            extraValue = null;
                        }
                    } else {
                        log.error(
                                "Message",
                                String.format(
                                        "Could not find a column with the name of %s",
                                        extraColumnName, index));
                        extraValue = null;
                    }
                }
                columnValuesPlus[i++] = extraValue;
            }
            return columnValuesPlus;
        } else {
            return columnValues;
        }
    }       


}