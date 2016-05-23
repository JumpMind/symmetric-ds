/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.sql;

public class SqlTemplateSettings {

    protected int fetchSize = 1000;
    protected int queryTimeout;
    protected int batchSize = 100;
    protected boolean readStringsAsBytes;
    protected int overrideIsolationLevel = -1;
    protected int resultSetType = java.sql.ResultSet.TYPE_FORWARD_ONLY;
    protected LogSqlBuilder logSqlBuilder;
    
    public SqlTemplateSettings() {     
    }      

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setReadStringsAsBytes(boolean readStringsAsBytes) {
        this.readStringsAsBytes = readStringsAsBytes;
    }
    
    public boolean isReadStringsAsBytes() {
        return readStringsAsBytes;
    }

    public int getOverrideIsolationLevel() {
        return overrideIsolationLevel;
    }

    public void setOverrideIsolationLevel(int overrideIsolationLevel) {
        this.overrideIsolationLevel = overrideIsolationLevel;
    }

    public LogSqlBuilder getLogSqlBuilder() {
        return logSqlBuilder;
    }

    public void setLogSqlBuilder(LogSqlBuilder logSqlBuilder) {
        this.logSqlBuilder = logSqlBuilder;
    }

    public int getResultSetType() {
        return resultSetType;
    }

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

}
