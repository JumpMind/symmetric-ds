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
package org.jumpmind.db.platform.ase;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.util.BinaryEncoding;

/*
 * The platform implementation for Sybase.
 */
public class AseDatabasePlatform extends AbstractJdbcDatabasePlatform {
    /* The standard Sybase jdbc driver. */
    public static final String JDBC_DRIVER = "com.sybase.jdbc4.jdbc.SybDriver";
    /* The old Sybase jdbc driver. */
    public static final String JDBC_DRIVER_OLD = "com.sybase.jdbc4.jdbc.SybDriver";
    /* The subprotocol used by the standard Sybase driver. */
    public static final String JDBC_SUBPROTOCOL = "sybase:Tds";
    /* The maximum size that text and binary columns can have. */
    public static final long MAX_TEXT_SIZE = 2147483647;
    private Map<String, String> sqlScriptReplacementTokens;

    public AseDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        sqlScriptReplacementTokens = super.getSqlScriptReplacementTokens();
        if (sqlScriptReplacementTokens == null) {
            sqlScriptReplacementTokens = new HashMap<String, String>();
        }
        sqlScriptReplacementTokens.put("current_timestamp", "getdate()");
        this.useMultiThreadSyncTriggers = false;
    }

    @Override
    protected AseDdlBuilder createDdlBuilder() {
        return new AseDdlBuilder();
    }

    @Override
    protected AseDdlReader createDdlReader() {
        return new AseDdlReader(this);
    }

    @Override
    protected AseJdbcSqlTemplate createSqlTemplate() {
        return new AseJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    public String getName() {
        return DatabaseNamesConstants.ASE;
    }

    public String getDefaultCatalog() {
        if (StringUtils.isBlank(defaultCatalog)) {
            defaultCatalog = getSqlTemplate().queryForObject("select DB_NAME()", String.class);
        }
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select USER_NAME()", String.class);
        }
        return defaultSchema;
    }

    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }

    @Override
    public boolean canColumnBeUsedInWhereClause(Column column) {
        return !isLob(column.getJdbcTypeCode()) && super.canColumnBeUsedInWhereClause(column);
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";
        String triggerSql = "create trigger TEST_TRIGGER on " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter
                + " for insert as begin select 1 end";
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
    protected Object getObjectValue(String value, Column column, BinaryEncoding encoding, boolean useVariableDates, boolean fitToColumn)
            throws DecoderException {
        Object objectValue = value;
        String typeName = column.getJdbcTypeName();
        if (typeName.equalsIgnoreCase("unichar") || typeName.equalsIgnoreCase("unitext") || typeName.equalsIgnoreCase("univarchar")) {
            String stringValue = cleanTextForTextBasedColumns((String) objectValue);
            int size = column.getSizeAsInt();
            if (settings.isRightTrimCharValues()) {
                stringValue = StringUtils.stripEnd(stringValue, null);
            }
            if (fitToColumn && size > 0 && stringValue.length() > size) {
                stringValue = stringValue.substring(0, size);
            }
            objectValue = stringValue;
            return objectValue;
        } else {
            return super.getObjectValue(value, column, encoding, useVariableDates, fitToColumn);
        }
    }
}
