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

import javax.sql.DataSource;

import org.jumpmind.symmetric.MultiDatabaseTestFactory.DatabaseRole;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

abstract public class AbstractDatabaseTest extends AbstractTest {

    private SymmetricEngine engine;
    
    File getSymmetricFile() {
        return MultiDatabaseTestFactory.writeTempPropertiesFileFor(TestConstants.MYSQL, DatabaseRole.ROOT);
    }
    
    protected SymmetricEngine getSymmetricEngine() {
        if (this.engine == null) {
            this.engine = createEngine(getSymmetricFile());
            dropAndCreateDatabaseTables(engine);
            ((IBootstrapService) this.engine.getApplicationContext().getBean(Constants.BOOTSTRAP_SERVICE)).init();
            new SqlScript(getResource(TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT), (DataSource) this.engine
                    .getApplicationContext().getBean(Constants.DATA_SOURCE), true).execute();
            this.engine.start();
        }
        return this.engine;
    }

    protected BeanFactory getBeanFactory() {
        return getSymmetricEngine().getApplicationContext();
    }

    protected DataSource getDataSource() {
        return (DataSource) getBeanFactory().getBean("dataSource");
    }

    protected JdbcTemplate getJdbcTemplate() {
        return new JdbcTemplate(getDataSource());
    }

    protected AbstractDbDialect getDbDialect() {
        return (AbstractDbDialect) getBeanFactory().getBean("dbDialect");
    }
    
    protected void cleanSlate(final String... tableName)
    {
        getJdbcTemplate().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection conn) throws SQLException, DataAccessException
            {
                Statement s = conn.createStatement();
                
                for (String table: tableName)
                {
                    s.executeUpdate("delete from "+table);
                }
                s.close();
                return null;
            }
            
        });
    }

}
