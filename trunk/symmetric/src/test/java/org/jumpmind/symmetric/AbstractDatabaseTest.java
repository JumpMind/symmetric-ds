/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.MultiDatabaseTest.DatabaseRole;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.ITest;

abstract public class AbstractDatabaseTest extends AbstractTest implements ITest {

    private Map<String, SymmetricEngine> engine = new HashMap<String, SymmetricEngine>();

    private String databaseType;

    public AbstractDatabaseTest(String dbType) {
        this.databaseType = dbType;
    }

    public AbstractDatabaseTest() {
    }

    public String getTestName() {
        return getDatabaseName();
    }

    File getSymmetricFile() {
        return MultiDatabaseTest.writeTempPropertiesFileFor(getDatabaseName(), DatabaseRole.ROOT);
    }

    protected SymmetricEngine getSymmetricEngine() {
        SymmetricEngine e = this.engine.get(getDatabaseName());
        if (e == null) {
            e = createEngine(getSymmetricFile());
            dropAndCreateDatabaseTables(getDatabaseName(), e);
            ((IBootstrapService) e.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE)).setupDatabase();
            new SqlScript(getResource(TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT), (DataSource) e
                    .getApplicationContext().getBean(Constants.DATA_SOURCE), true).execute();
            e.start();
            this.engine.put(getDatabaseName(), e);
        }
        return e;
    }

    protected String getDatabaseName() {
        if (databaseType == null) {
            Properties properties = MultiDatabaseTest.getTestProperties();
            String[] rootDatabaseTypes = StringUtils.split(properties.getProperty("test.root"), ",");
            databaseType = rootDatabaseTypes[0];
        }
        return databaseType;
    }

    protected BeanFactory getBeanFactory() {
        return getSymmetricEngine().getApplicationContext();
    }

    protected DataSource getDataSource() {
        return (DataSource) getBeanFactory().getBean(Constants.DATA_SOURCE);
    }

    protected JdbcTemplate getJdbcTemplate() {
        return new JdbcTemplate(getDataSource());
    }

    protected AbstractDbDialect getDbDialect() {
        return (AbstractDbDialect) getBeanFactory().getBean(Constants.DB_DIALECT);
    }

    protected IParameterService getParameterService() {
        return (IParameterService) getBeanFactory().getBean(Constants.PARAMETER_SERVICE);
    }

    protected void cleanSlate(final String... tableName) {
        getJdbcTemplate().execute(new ConnectionCallback() {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException {
                Statement s = conn.createStatement();

                for (String table : tableName) {
                    s.executeUpdate("delete from " + table);
                }
                s.close();
                return null;
            }

        });
    }

}
