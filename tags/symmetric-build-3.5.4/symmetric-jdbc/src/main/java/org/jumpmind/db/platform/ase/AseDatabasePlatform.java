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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.JdbcUtils;
import org.jumpmind.db.sql.SqlTemplateSettings;

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

        sqlScriptReplacementTokens = new HashMap<String, String>();
        sqlScriptReplacementTokens.put("current_timestamp", "getdate()");
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
        return new AseJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo(), JdbcUtils.getNativeJdbcExtractory());
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
            defaultSchema = (String) getSqlTemplate().queryForObject("select USER_NAME()",
                    String.class);
        }
        return defaultSchema;
    }

    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }
}
