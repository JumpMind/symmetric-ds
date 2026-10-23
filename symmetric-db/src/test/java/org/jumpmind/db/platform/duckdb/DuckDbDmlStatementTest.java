/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Affero General Public License, version 3.0 (AGPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU Affero General Public License,
 * version 3.0 (AGPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.platform.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.DmlStatementOptions;
import org.junit.jupiter.api.Test;

class DuckDbDmlStatementTest {
    @Test
    void testBuildInsertSql_withKeyColumns_appendsOnConflictDoNothing() {
        DuckDbDmlStatement statement = newStatement(DmlType.INSERT, tableWithPrimaryKey());
        assertEquals("insert into item (id, name) values (?,?) on conflict do nothing", statement.getSql());
    }

    @Test
    void testBuildInsertSql_withoutKeyColumns_appendsOnConflictDoNothing() {
        DuckDbDmlStatement statement = newStatement(DmlType.INSERT, tableWithoutPrimaryKey());
        assertEquals("insert into item (id, name) values (?,?) on conflict do nothing", statement.getSql());
    }

    private DuckDbDmlStatement newStatement(DmlType dmlType, Table table) {
        DmlStatementOptions options = new DmlStatementOptions(dmlType, table)
                .databaseInfo(new DuckDbDdlBuilder().getDatabaseInfo());
        return new DuckDbDmlStatement(options);
    }

    private Table tableWithPrimaryKey() {
        Column id = new Column("id", true, Types.INTEGER, 10, 0);
        Column name = new Column("name", false, Types.VARCHAR, 50, 0);
        return new Table("item", id, name);
    }

    private Table tableWithoutPrimaryKey() {
        Column id = new Column("id", false, Types.INTEGER, 10, 0);
        Column name = new Column("name", false, Types.VARCHAR, 50, 0);
        return new Table("item", id, name);
    }
}
