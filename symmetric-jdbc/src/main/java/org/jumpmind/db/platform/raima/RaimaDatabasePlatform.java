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
package org.jumpmind.db.platform.raima;
import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class RaimaDatabasePlatform extends AbstractJdbcDatabasePlatform {

    public static final String JDBC_DRIVER = "com.raima.rdm.jdbc.RDMDriverr";

    public static final String JDBC_SUBPROTOCOL = "raima";
    
    public RaimaDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    protected RaimaDdlBuilder createDdlBuilder() {
        return new RaimaDdlBuilder();
    }

    @Override
    protected RaimaDdlReader createDdlReader() {
        return new RaimaDdlReader(this);
    }

    @Override
    protected RaimaJdbcSqlTemplate createSqlTemplate() {
        return new RaimaJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    @Override
    protected ISqlTemplate createSqlTemplateDirty() {
        return sqlTemplate;
    }
    
    public String getName() {
        return DatabaseNamesConstants.RAIMA;
    }

    public String getDefaultSchema() {
        return null;
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";

        String triggerSql = "CREATE TRIGGER TEST_TRIGGER FOR"+ delimiter + PERMISSION_TEST_TABLE_NAME + delimiter
                + " AFTER UPDATE FOR EACH ROW AS INSERT INTO " +  delimiter + PERMISSION_TEST_TABLE_NAME + delimiter + " VALUES(NULL,NULL); END_TRIGGER";

        String dropTriggerSql = "DROP TRIGGER IF EXISTS TEST_TRIGGER";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, Status.FAIL);

        try {
            //getSqlTemplate().update(dropTriggerSql);
            //getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
            //getSqlTemplate().update(dropTriggerSql);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission or TRIGGER permission");
        }

        return result;
    }

    @Override
    public PermissionResult getCreateSymRoutinePermission() {
        String routineSql = "CREATE PROCEDURE TEST_PROC() AS VAR myVar = 1; END_PROCEDURE";
        String dropSql = "DROP PROCEDURE IF EXISTS TEST_PROC";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_ROUTINE, Status.FAIL);

        try {
            //getSqlTemplate().update(dropSql);
            //getSqlTemplate().update(routineSql);
            result.setStatus(Status.PASS);
            //getSqlTemplate().update(dropSql);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE ROUTINE Privilege");
        }
        return result;
    }
    
    @Override
    protected PermissionResult getDropSymTriggerPermission() {
        String dropTriggerSql = "DROP TRIGGER IF EXISTS TEST_TRIGGER";
        PermissionResult result = new PermissionResult(PermissionType.DROP_TRIGGER, Status.FAIL);

        try {
            //getSqlTemplate().update(dropTriggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant DROP TRIGGER permission or TRIGGER permission");
        }

        return result;
    }
}
