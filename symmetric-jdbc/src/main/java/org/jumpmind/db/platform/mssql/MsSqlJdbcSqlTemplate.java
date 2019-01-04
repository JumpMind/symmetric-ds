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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MsSqlJdbcSqlTemplate extends JdbcSqlTemplate {

    public MsSqlJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, DatabaseInfo databaseInfo) {
        super(dataSource, settings, null, databaseInfo);
        primaryKeyViolationCodes = new int[] {2627, 2601};
        uniqueKeyViolationNameRegex = new String[] { "with unique index '(.*)'" };
        foreignKeyViolationCodes = new int[] {547};
        foreignKeyChildExistsViolationMessageParts = new String[] { 
                "DELETE statement conflicted with the SAME TABLE REFERENCE constraint",
                "DELETE statement conflicted with the REFERENCE constraint",
                "UPDATE statement conflicted with the SAME TABLE REFERENCE constraint",
                "UPDATE statement conflicted with the REFERENCE constraint" };
    }
    
    @Override
    public ISqlTransaction startSqlTransaction() {
        return new MsSqlJdbcSqlTransaction(this);
    }
   
    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
    
    @Override
    protected void setTinyIntValue(PreparedStatement ps, int i, Object arg, int argType)
            throws SQLException {
        super.setTinyIntValue(ps, i, arg, Types.SMALLINT);
    }
    
}
