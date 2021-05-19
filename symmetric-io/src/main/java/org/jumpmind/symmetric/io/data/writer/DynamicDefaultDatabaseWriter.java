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
package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.io.data.DataContext;

public class DynamicDefaultDatabaseWriter extends DefaultDatabaseWriter {

    private IDatabasePlatform targetPlatform;
    
    private ISqlTransaction targetTransaction;
    
    private String tablePrefix;
    
    public DynamicDefaultDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, String prefix) {
        super(symmetricPlatform);
        this.tablePrefix = prefix.toLowerCase();
        this.targetPlatform = targetPlatform;
    }
    
    public DynamicDefaultDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
            String prefix, DatabaseWriterSettings settings) {
        super(symmetricPlatform, null, settings);
        this.tablePrefix = prefix.toLowerCase();
        this.targetPlatform = targetPlatform;
    }

    public DynamicDefaultDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, String prefix, 
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        super(symmetricPlatform, conflictResolver, settings);
        this.tablePrefix = prefix.toLowerCase();
        this.targetPlatform = targetPlatform;
    }
    
    protected boolean isSymmetricTable(String tableName) {
        return tableName.toUpperCase().startsWith(tablePrefix.toUpperCase());
    }
    
    public boolean isLoadOnly() {
        return !platform.equals(targetPlatform);
    }
    
    @Override
    public IDatabasePlatform getPlatform(Table table) {
        if (table == null) {
            table = targetTable;
        }
        return table == null || isSymmetricTable(table.getNameLowerCase()) ? platform : targetPlatform;
    }
    
    @Override
    public IDatabasePlatform getPlatform(String table) {
        if (table == null) {
            table = targetTable.getNameLowerCase();
        } else {
            table = table.toLowerCase();
        }
        return table == null || isSymmetricTable(table) ? platform : targetPlatform;
    }

    @Override
    public IDatabasePlatform getPlatform() {
        return targetTable == null || isSymmetricTable(targetTable.getNameLowerCase()) ? super.platform : targetPlatform;
    }
    
    @Override
    public ISqlTransaction getTransaction() {
        return targetTable == null || isSymmetricTable(targetTable.getNameLowerCase()) || targetTransaction == null ?
                super.transaction : targetTransaction;
    }
    
    @Override
    public ISqlTransaction getTransaction(Table table) {
        if (table == null) {
            table = targetTable;
        }
        if (targetTransaction == null) {
            return transaction;
        }
        return table == null || isSymmetricTable(table.getNameLowerCase()) ? transaction : targetTransaction;
    }
    
    @Override
    public ISqlTransaction getTransaction(String table) {
        if (table == null) {
            table = targetTable.getNameLowerCase();
        } else {
            table = table.toLowerCase();
        }
        if (targetTransaction == null) {
            return transaction;
        }
        return table == null || isSymmetricTable(table.toLowerCase()) ? transaction : targetTransaction;
    }
    
    public ISqlTransaction getTargetTransaction() {
        return targetTransaction == null ? transaction : targetTransaction;
    }

    @Override
    public void open(DataContext context) {
        super.open(context);
        if (isLoadOnly()) {
            this.targetTransaction = targetPlatform.getSqlTemplate().startSqlTransaction(!targetPlatform.supportsTransactions());
        }
    }
    
    @Override
    public void close() {
        super.close();
        if (isLoadOnly() && targetTransaction != null) {
            targetTransaction.close();
        }
    }
    
    @Override
    protected void commit(boolean earlyCommit) {
        super.commit(earlyCommit);
        if (isLoadOnly() && targetTransaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
                targetTransaction.commit();
                if (!earlyCommit) {
                   notifyFiltersBatchCommitted();
                } else {
                    notifyFiltersEarlyCommit();
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
            }
        }
    }
    
    @Override
    protected void rollback() {
        super.rollback();
        if (isLoadOnly() && targetTransaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
                targetTransaction.rollback();
                notifyFiltersBatchRolledback();
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
            }
        }
    }

    public String getTablePrefix() {
        return tablePrefix;
    }
    
}
