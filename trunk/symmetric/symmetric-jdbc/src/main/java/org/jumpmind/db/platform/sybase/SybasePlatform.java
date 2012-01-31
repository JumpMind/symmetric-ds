package org.jumpmind.db.platform.sybase;

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

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabasePlatformSettings;

/*
 * The platform implementation for Sybase.
 */
public class SybasePlatform extends AbstractJdbcDatabasePlatform {

    /* Database name of this platform. */
    public static final String DATABASENAME = "Sybase";

    /* The standard Sybase jdbc driver. */
    public static final String JDBC_DRIVER = "com.sybase.jdbc2.jdbc.SybDriver";

    /* The old Sybase jdbc driver. */
    public static final String JDBC_DRIVER_OLD = "com.sybase.jdbc.SybDriver";

    /* The subprotocol used by the standard Sybase driver. */
    public static final String JDBC_SUBPROTOCOL = "sybase:Tds";

    /* The maximum size that text and binary columns can have. */
    public static final long MAX_TEXT_SIZE = 2147483647;
    
    private Map<String, String> sqlScriptReplacementTokens;

    public SybasePlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);

        info.setMaxIdentifierLength(128);
        info.setNullAsDefaultValueRequired(true);
        info.setCommentPrefix("/*");
        info.setCommentSuffix("*/");
        info.setDelimiterToken("\"");

        info.addNativeTypeMapping(Types.ARRAY, "IMAGE");
        // BIGINT is mapped back in the model reader
        info.addNativeTypeMapping(Types.BIGINT, "DECIMAL(19,0)");
        // we're not using the native BIT type because it is rather limited
        // (cannot be NULL, cannot be indexed)
        info.addNativeTypeMapping(Types.BIT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.BLOB, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DATE, "DATETIME", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DISTINCT, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.INTEGER, "INT");
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "IMAGE");
        info.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT");
        info.addNativeTypeMapping(Types.NULL, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TIME, "DATETIME", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");
        info.addNativeTypeMapping("DATALINK", "IMAGE", "LONGVARBINARY");

        info.setDefaultSize(Types.BINARY, 254);
        info.setDefaultSize(Types.VARBINARY, 254);
        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);

        info.setDateOverridesToTimestamp(true);
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);
        info.setAutoIncrementUpdateAllowed(false);
        
        ddlReader = new SybaseDdlReader(this);
        ddlBuilder = new SybaseBuilder(this);
        
        sqlScriptReplacementTokens = new HashMap<String, String>();
        sqlScriptReplacementTokens.put("current_timestamp", "getdate()");
    }

    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new SybaseJdbcSqlTemplate(dataSource, settings, null);
    }
    
    public String getName() {
        return DATABASENAME;
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
