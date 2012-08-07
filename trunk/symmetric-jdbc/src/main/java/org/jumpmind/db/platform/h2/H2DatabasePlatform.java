package org.jumpmind.db.platform.h2;

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
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the H2 database.
 */
public class H2DatabasePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    /* The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.h2.Driver";

    /* The sub protocol used by the H2 driver. */
    public static final String JDBC_SUBPROTOCOL = "h2";

    /*
     * Creates a new instance of the H2 platform.
     */
    public H2DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
    
    @Override
    protected H2DdlBuilder createDdlBuilder() {
        return  new H2DdlBuilder();
    }

    @Override
    protected H2DdlReader createDdlReader() {
        return new H2DdlReader(this);
    }        
    
    @Override
    protected H2JdbcSqlTemplate createSqlTemplate() {
        return new H2JdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    public String getName() {
        return DatabaseNamesConstants.H2;
    }
    
    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select SCHEMA()", String.class);
        }
        return defaultSchema;
    }
    
    public String getDefaultCatalog() {
        return null;
    }

}