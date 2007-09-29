package org.jumpmind.symmetric;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.TestConstants;
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
                .getMySqlTestEngine1(TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT);
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
