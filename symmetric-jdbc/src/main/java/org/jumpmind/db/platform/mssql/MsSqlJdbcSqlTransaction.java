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
package org.jumpmind.db.platform.mssql;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;

public class MsSqlJdbcSqlTransaction extends JdbcSqlTransaction {
    private boolean allowsAutoIncrement;

    public MsSqlJdbcSqlTransaction(JdbcSqlTemplate sqltemplate) {
        super(sqltemplate);
    }

    public MsSqlJdbcSqlTransaction(JdbcSqlTemplate sqltemplate, boolean autoCommit) {
        super(sqltemplate, autoCommit);
    }

    @Override
    public void allowInsertIntoAutoIncrementColumns(boolean allow, Table table, String quote, String catalogSeparator, String schemaSeparator) {
        if (table != null && table.getAutoIncrementColumns().length > 0) {
            if (allow) {
                execute(String.format("SET IDENTITY_INSERT %s ON",
                        table.getQualifiedTableName(quote, catalogSeparator, schemaSeparator)));
                this.allowsAutoIncrement = true;
            } else {
                execute(String.format("SET IDENTITY_INSERT %s OFF",
                        table.getQualifiedTableName(quote, catalogSeparator, schemaSeparator)));
                this.allowsAutoIncrement = false;
            }
        }
    }

    @Override
    public boolean isAllowInsertIntoAutoIncrement() {
        return this.allowsAutoIncrement;
    }
}
