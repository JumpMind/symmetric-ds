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
package org.jumpmind.db.platform.redshift;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;

public class RedshiftJdbcSqlTemplate extends JdbcSqlTemplate {
    public RedshiftJdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings, SymmetricLobHandler lobHandler,
            DatabaseInfo databaseInfo) {
        super(dataSource, settings, lobHandler, databaseInfo);
        this.requiresAutoCommitFalseToSetFetchSize = true;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    @Override
    protected String getSelectLastInsertIdSql(String sequenceName) {
        if (sequenceName.equals("sym_data_data_id")) {
            return "select max(data_id) from sym_data";
        }
        throw new UnsupportedOperationException();
    }
}
