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

import java.sql.Types;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the Microsoft SQL Server 2000 database.
 */
public class MsSql2000DatabasePlatform extends AbstractJdbcDatabasePlatform {
    /* The standard SQLServer jdbc driver. */
    public static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
    /* The sub protocol used by the standard SQL Server driver. */
    public static final String JDBC_SUBPROTOCOL = "jtds";
    int engineEdition = -1;

    /*
     * Creates a new platform instance.
     */
    public MsSql2000DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        supportsTruncate = false;
    }

    @Override
    protected IDdlBuilder createDdlBuilder() {
        return new MsSql2000DdlBuilder();
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
            defaultCatalog = (String) getSqlTemplate().queryForObject("select DB_NAME()", String.class);
        }
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select current_user", String.class);
        }
        return defaultSchema;
    }

    @Override
    public boolean allowsUniqueIndexDuplicatesWithNulls() {
        return false;
    }

    @Override
    public boolean isClob(int type) {
        return super.isClob(type) ||
        // SQL-Server ntext binary type
                type == -10;
    }

    @Override
    public boolean canColumnBeUsedInWhereClause(Column column) {
        if (column.getMappedTypeCode() == Types.VARBINARY && column.getSizeAsInt() > 8000) {
            return false;
        }
        return !isLob(column.getJdbcTypeCode()) && super.canColumnBeUsedInWhereClause(column);
    }

    @Override
    protected Object parseFloat(String value) {
        return cleanNumber(value).replace(',', '.');
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";
        String triggerSql = "CREATE TRIGGER TEST_TRIGGER ON " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter
                + " AFTER UPDATE AS SELECT 1 GO";
        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, triggerSql);
        try {
            getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission or TRIGGER permission");
        }
        return result;
    }

    @Override
    public PermissionResult getCreateSymFunctionPermission() {
        String routineSql = "CREATE FUNCTION TEST_FUNC() RETURNS INTEGER BEGIN RETURN 1; END";
        String dropSql = "IF OBJECT_ID('TEST_FUNC') IS NOT NULL DROP FUNCTION TEST_FUNC";
        PermissionResult result = new PermissionResult(PermissionType.CREATE_FUNCTION,
                dropSql + "\r\n" + routineSql + "\r\n" + dropSql);
        try {
            getSqlTemplate().update(dropSql);
            getSqlTemplate().update(routineSql);
            getSqlTemplate().update(dropSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            if (result.getSolution() != null) {
                result.setSolution("Grant CREATE FUNCTION Privilege");
            }
        }
        return result;
    }

    @Override
    public PermissionResult getLogMinePermission() {
        final PermissionResult result = new PermissionResult(PermissionType.LOG_MINE, "");
        result.setSolution("Change Tracking not available");
        result.setStatus(Status.FAIL);
        return result;
    }

    @Override
    public String massageForObjectAlreadyExists(String sql) {
        if (sql.toUpperCase().contains("CREATE TABLE")) {
            return sql;
        }
        return StringUtils.replaceOnceIgnoreCase(sql, "create", "alter");
    }

    @Override
    public String massageForObjectDoesNotExist(String sql) {
        if (sql.toUpperCase().contains("ALTER TABLE") || sql.toUpperCase().contains(" OR ALTER ")) {
            return sql;
        }
        return StringUtils.replaceOnceIgnoreCase(sql, "alter", "create");
    }

    public int getEngineEdition() {
        if (engineEdition < 0) {
            try {
                engineEdition = this.sqlTemplate.queryForInt("SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT)");
            } catch (Exception e) {
                engineEdition = 0;
                // Not supported until MSSQL 2016
            }
        }
        return engineEdition;
    }
}
