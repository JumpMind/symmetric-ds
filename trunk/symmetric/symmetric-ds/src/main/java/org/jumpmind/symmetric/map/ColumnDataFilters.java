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

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.springframework.scripting.ScriptCompilationException;

/**
 * 
 */
public class ColumnDataFilters implements IDataLoaderFilter, INodeGroupExtensionPoint {

    final ILog log = LogFactory.getLog(getClass());

    private boolean autoRegister = true;

    private String[] nodeGroupIdsToApplyTo;

    List<TableColumnValueFilter> filters;

    private boolean ignoreCase = true;

    private boolean enabled = true;

    protected void filterColumnValues(IDataLoaderContext context, String[] columnValues) {
        if (enabled && filters != null) {
            for (TableColumnValueFilter filteredColumn : filters) {
                try {
                    if (filteredColumn.isEnabled()
                            && ((ignoreCase && filteredColumn.getTableName().equalsIgnoreCase(
                                    context.getTableName())) || (!ignoreCase && filteredColumn
                                    .getTableName().equals(context.getTableName())))) {
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
                                        filteredColumn.getTableName(),
                                        filteredColumn.getColumnName(), columnValues[index],
                                        context.getContextCache());
                            } catch (RuntimeException ex) {
                                // Try to log script errors so they are more
                                // readable
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
                } catch (RuntimeException ex) {
                    log.error("ColumnDataFilterError", filteredColumn.getColumnName(),
                            filteredColumn.getTableName());
                    throw ex;
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

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

}