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

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the Microsoft SQL Server 2000 database.
 */
public class MsSql2000DatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard SQLServer jdbc driver. */
    public static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    /* The sub protocol used by the standard SQL Server driver. */
    public static final String JDBC_SUBPROTOCOL = "jtds";

    /*
     * Creates a new platform instance.
     */
    public MsSql2000DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    protected IDdlBuilder createDdlBuilder() {      
       return new MsSql2000DdlBuilder(getName());    
    }

    @Override
    protected MsSqlDdlReader createDdlReader() {
        return new MsSqlDdlReader(this);
    }    
    
    @Override
    protected MsSqlJdbcSqlTemplate createSqlTemplate() {
        return new MsSqlJdbcSqlTemplate(dataSource, settings, getDatabaseInfo());
    }

    public String getName() {
        return DatabaseNamesConstants.MSSQL2000;
    }

    public String getDefaultCatalog() {
        if (StringUtils.isBlank(defaultCatalog)) {
            defaultCatalog = (String) getSqlTemplate().queryForObject("select DB_NAME()",
                    String.class);
        }
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select current_user",
                    String.class);
        }
        return defaultSchema;
    }

    @Override
    public boolean isClob(int type) {
        return super.isClob(type) ||
        // SQL-Server ntext binary type
                type == -10;
    }

    @Override
    public boolean canColumnBeUsedInWhereClause(Column column) {
        return !column.isOfBinaryType();
    }

    @Override
    protected Object parseFloat(String value) {
        return cleanNumber(value);
    }
}


