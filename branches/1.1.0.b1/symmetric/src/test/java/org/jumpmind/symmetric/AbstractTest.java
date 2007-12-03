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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.BeforeTest;

abstract public class AbstractTest {

    protected SymmetricEngine engine;

    @BeforeTest(groups = "continuous")
    synchronized public void init() throws Exception {
        engine = SymmetricEngineTestFactory
                .getContinuousTestEngine();
    }

    protected BeanFactory getBeanFactory() {
        return engine.getApplicationContext();
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
                return null;
            }
            
        });
    }

}
