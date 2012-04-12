package org.jumpmind.db.platform.db2;

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

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.DatabasePlatformSettings;

/*
 * The DB2 platform implementation.
 */
public class Db2DatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard DB2 jdbc driver. */
    public static final String JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver";

    /* The subprotocol used by the standard DB2 driver. */
    public static final String JDBC_SUBPROTOCOL = "db2";    

    /*
     * Creates a new platform instance.
     */
    public Db2DatabasePlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);

        // the BINARY types are also handled by Db2Builder.getSqlType(Column)
        info.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.BINARY, "CHAR {0} FOR BIT DATA");
        info.addNativeTypeMapping(Types.BIT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "LONG VARCHAR FOR BIT DATA");
        info.addNativeTypeMapping(Types.LONGVARCHAR, "LONG VARCHAR");
        info.addNativeTypeMapping(Types.NULL, "LONG VARCHAR FOR BIT DATA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.NUMERIC, "DECIMAL", Types.DECIMAL);
        info.addNativeTypeMapping(Types.OTHER, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY, "VARCHAR {0} FOR BIT DATA");
        info.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");

        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);
        info.setDefaultSize(Types.BINARY, 254);
        info.setDefaultSize(Types.VARBINARY, 254);
        

        info.setMaxIdentifierLength(128);
        info.setMaxColumnNameLength(128);
        info.setMaxConstraintNameLength(128);
        info.setMaxForeignKeyNameLength(128);
        
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);
        
        ddlReader = new Db2DdlReader(this);
        ddlBuilder = new Db2DdlBuilder(this);
    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new Db2JdbcSqlTemplate(dataSource, settings, null);
    }

    public String getName() {
        return DatabaseNamesConstants.DB2;
    }
    
    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("values CURRENT SCHEMA", String.class);
        }
        return defaultSchema;
    }
    
    public String getDefaultCatalog() {
        return null;
    }
    
}
