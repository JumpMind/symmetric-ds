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
package org.jumpmind.symmetric.load;


abstract public class AbstractTableDataLoaderFilter implements INodeGroupDataLoaderFilter {

    private boolean autoRegister = true;

    private String[] nodeGroupIdsToApplyTo;

    private boolean loadDataInTargetDatabase = true;

    private String tableName;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        if (context.getTableName().equals(tableName)) {
            filterDeleteForTable(context, keyValues);
        }
        return loadDataInTargetDatabase;
    }

    abstract protected void filterDeleteForTable(IDataLoaderContext context, String[] keyValues);

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        if (context.getTableName().equals(tableName)) {
            filterInsertForTable(context, columnValues);
        }
        return loadDataInTargetDatabase;
    }

    abstract protected void filterInsertForTable(IDataLoaderContext context, String[] columnValues);

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        if (context.getTableName().equals(tableName)) {
            filterUpdateForTable(context, columnValues, keyValues);
        }
        return loadDataInTargetDatabase;
    }

    abstract protected void filterUpdateForTable(IDataLoaderContext context, String[] columnValues, String[] keyValues);

    public boolean isAutoRegister() {
        return this.autoRegister;
    }

    public String[] getNodeGroupIdsToApplyTo() {
        return this.nodeGroupIdsToApplyTo;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public void setNodeGroupIdsToApplyTo(String[] nodeGroupIdsToApplyTo) {
        this.nodeGroupIdsToApplyTo = nodeGroupIdsToApplyTo;
    }

    public void setNodeGroupIdToApplyTo(String nodeGroupId) {
        this.nodeGroupIdsToApplyTo = new String[] { nodeGroupId };
    }

    public void setLoadDataInTargetDatabase(boolean loadDataInTargetDatabase) {
        this.loadDataInTargetDatabase = loadDataInTargetDatabase;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}