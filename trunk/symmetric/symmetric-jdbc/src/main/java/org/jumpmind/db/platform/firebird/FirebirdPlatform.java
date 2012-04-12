package org.jumpmind.db.platform.firebird;

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

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.DatabasePlatformInfo;
import org.jumpmind.db.platform.DatabasePlatformSettings;

/*
 * The platform implementation for the Firebird database.
 * It is assumed that the database is configured with sql dialect 3!
 */
public class FirebirdPlatform extends AbstractJdbcDatabasePlatform {
    /* Database name of this platform. */
    public static final String DATABASENAME = DatabaseNamesConstants.FIREBIRD;

    /* The standard Firebird jdbc driver. */
    public static final String JDBC_DRIVER = "org.firebirdsql.jdbc.FBDriver";

    /* The subprotocol used by the standard Firebird driver. */
    public static final String JDBC_SUBPROTOCOL = "firebirdsql";

    /*
     * Creates a new Firebird platform instance.
     */
    public FirebirdPlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);

        DatabasePlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(31);
        info.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        info.setCommentPrefix("/*");
        info.setCommentSuffix("*/");

        info.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BINARY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BIT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.CLOB, "BLOB SUB_TYPE TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BLOB, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARCHAR, "BLOB SUB_TYPE TEXT");
        info.addNativeTypeMapping(Types.NULL, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER, "BLOB", Types.LONGVARBINARY);
        // This is back-mapped to REAL in the model reader
        info.addNativeTypeMapping(Types.REAL, "FLOAT");
        info.addNativeTypeMapping(Types.REF, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY, "BLOB", Types.LONGVARBINARY);

        info.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");
        info.addNativeTypeMapping("DATALINK", "BLOB", "LONGVARBINARY");

        info.setDefaultSize(Types.VARCHAR, 254);
        info.setDefaultSize(Types.CHAR, 254);

        
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);

        ddlReader = new FirebirdDdlReader(this);
        ddlBuilder = new FirebirdBuilder(this);
    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new FirebirdJdbcSqlTemplate(dataSource, settings, null);   
    }

    public String getName() {
        return DATABASENAME;
    }
    
    public String getDefaultCatalog() {
        return null;
    }
    
    public String getDefaultSchema() {
        return null;
    }
    
}
