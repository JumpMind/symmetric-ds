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
package org.jumpmind.db.platform.nuodb;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for MySQL.
 */
public class NuoDbDatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard NuoDB jdbc driver. */
    public static final String JDBC_DRIVER = "com.nuodb.jdbc.Driver";

    /* The subprotocol used by the standard MySQL driver. */
    public static final String JDBC_SUBPROTOCOL = "nuodb";
    
    private Map<String, String> sqlScriptReplacementTokens;

    /*
     * Creates a new platform instance.
     */
    public NuoDbDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        
        String delimiter = getDatabaseInfo().getDelimiterToken();
        sqlScriptReplacementTokens = new HashMap<String,String>();
        sqlScriptReplacementTokens.put("\\$\\(schemaName\\)", delimiter + getDefaultSchema() + delimiter + "." );
    }

    @Override
    protected NuoDbDdlBuilder createDdlBuilder() {
        return new NuoDbDdlBuilder(this);
    }

    @Override
    protected NuoDbDdlReader createDdlReader() {
        return new NuoDbDdlReader(this);
    }

    @Override
    protected NuoDbJdbcSqlTemplate createSqlTemplate() {
        return new NuoDbJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    @Override
    protected ISqlTemplate createSqlTemplateDirty() {
        return sqlTemplate;
    }
    
    public String getName() {
        return DatabaseNamesConstants.NUODB;
    }

    public String getDefaultSchema() {
        if(StringUtils.isBlank(defaultSchema)){
            defaultSchema = getSqlTemplate().queryForObject("select database() from dual", String.class);
        }
        return defaultSchema;
    }

    public String getDefaultCatalog() {
        return null;
    }
    
    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";

        String triggerSql = "CREATE TRIGGER TEST_TRIGGER FOR " + delimiter + defaultSchema + delimiter + "." + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter
                + " AFTER UPDATE FOR EACH ROW AS INSERT INTO " + delimiter + defaultSchema + delimiter + "." + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter + " VALUES(NULL,NULL); END_TRIGGER";

        String dropTriggerSql = "DROP TRIGGER IF EXISTS " + delimiter + defaultSchema + delimiter + ".TEST_TRIGGER";
        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, Status.FAIL);

        try {
            getSqlTemplate().update(dropTriggerSql);
            getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
            getSqlTemplate().update(dropTriggerSql);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission or TRIGGER permission");
        }

        return result;
    }

    @Override
    public PermissionResult getCreateSymRoutinePermission() {
        String routineSql = "CREATE PROCEDURE " + defaultSchema + ".TEST_PROC() AS VAR myVar = 1; END_PROCEDURE";
        String dropSql = "DROP PROCEDURE IF EXISTS " + defaultSchema + ".TEST_PROC";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_ROUTINE, Status.FAIL);

        try {
            getSqlTemplate().update(dropSql);
            getSqlTemplate().update(routineSql);
            result.setStatus(Status.PASS);
            getSqlTemplate().update(dropSql);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE ROUTINE Privilege");
        }
        return result;
    }
    
    @Override
    protected PermissionResult getDropSymTriggerPermission() {
        String dropTriggerSql = "DROP TRIGGER IF EXISTS " + getDefaultSchema() + ".TEST_TRIGGER";
        PermissionResult result = new PermissionResult(PermissionType.DROP_TRIGGER, Status.FAIL);

        try {
            getSqlTemplate().update(dropTriggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant DROP TRIGGER permission or TRIGGER permission");
        }

        return result;
    }
}
