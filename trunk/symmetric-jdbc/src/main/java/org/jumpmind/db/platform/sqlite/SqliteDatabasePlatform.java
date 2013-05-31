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
package org.jumpmind.db.platform.sqlite;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class SqliteDatabasePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    /* The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.sqlite.JDBC";
    
    private Map<String, String> sqlScriptReplacementTokens;
    
    public SqliteDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        sqlScriptReplacementTokens = new HashMap<String, String>();
        sqlScriptReplacementTokens.put("current_timestamp", "strftime('%Y-%m-%d %H:%M:%f','now','localtime')");
        sqlScriptReplacementTokens.put("\\{ts([^<]*?)\\}","$1");
        sqlScriptReplacementTokens.put("\\{d([^<]*?)\\}","$1");
    }

    
    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }
    
    public String getName() {
        return DatabaseNamesConstants.SQLITE;
    }

    public String getDefaultSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getDefaultCatalog() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    protected SqliteDdlBuilder createDdlBuilder() {
        return  new SqliteDdlBuilder();
    }

    @Override
    protected SqliteDdlReader createDdlReader() {
        return new SqliteDdlReader(this);
    }    
    
    
    protected ISqlTemplate createSqlTemplate() {
        return new SqliteJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo()); 
    }
}
