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

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.SqlTemplateSettings;

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
    public Db2DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);        
    }
    
    @Override
    protected Db2DdlBuilder createDdlBuilder() {
        return new Db2DdlBuilder();
    }

    @Override
    protected Db2DdlReader createDdlReader() {
        return new Db2DdlReader(this);
    }

    @Override
    protected Db2JdbcSqlTemplate createSqlTemplate() {
        return new Db2JdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
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
        return "";
    }
    
}
