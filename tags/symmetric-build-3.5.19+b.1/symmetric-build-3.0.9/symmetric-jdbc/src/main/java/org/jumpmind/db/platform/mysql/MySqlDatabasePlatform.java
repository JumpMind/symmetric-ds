package org.jumpmind.db.platform.mysql;

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
 * The platform implementation for MySQL.
 */
public class MySqlDatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard MySQL jdbc driver. */
    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    /* The old MySQL jdbc driver. */
    public static final String JDBC_DRIVER_OLD = "org.gjt.mm.mysql.Driver";

    /* The subprotocol used by the standard MySQL driver. */
    public static final String JDBC_SUBPROTOCOL = "mysql";

    /*
     * Creates a new platform instance.
     */
    public MySqlDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, overrideSettings(settings));
        ddlReader = new MySqlDdlReader(this);
        ddlBuilder = new MySqlDdlBuilder();
    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new MySqlJdbcSqlTemplate(dataSource, settings, null);
    }
    
    /*
     * According to the documentation (and experience) the jdbc driver for mysql
     * requires the fetch size to be as follows.
     */
    protected static SqlTemplateSettings overrideSettings(SqlTemplateSettings settings) {
        if (settings == null) {
            settings = new SqlTemplateSettings();
        }
        settings.setFetchSize(Integer.MIN_VALUE);
        return settings;
    }

    public String getName() {
        return DatabaseNamesConstants.MYSQL;
    }
    
    public String getDefaultSchema() {
        return null;
    }
    
    public String getDefaultCatalog() {  
        if (StringUtils.isBlank(defaultCatalog)) {
            defaultCatalog = getSqlTemplate().queryForObject("select database()", String.class);
        }
        return defaultCatalog;
    }


}
