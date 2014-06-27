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


package org.jumpmind.symmetric.db;

import java.util.ArrayList;

import org.apache.commons.collections.CollectionUtils;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
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

    public String[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table,
            String[] columnValues) {
        if (dml == DmlType.UPDATE && indexesToRemove != null) {
            ArrayList<String> values = new ArrayList<String>();
            CollectionUtils.addAll(values, columnValues);
            for (int index : indexesToRemove) {
                if (index >= 0) {
                    values.remove(index);
                }
            }
            return values.toArray(new String[values.size()]);
        }
        return columnValues;
    }

    public boolean isAutoRegister() {
        return false;
    }
}