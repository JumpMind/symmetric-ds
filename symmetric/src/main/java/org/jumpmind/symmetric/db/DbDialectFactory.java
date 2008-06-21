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

package org.jumpmind.symmetric.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.platform.derby.DerbyPlatform;
import org.apache.ddlutils.platform.firebird.FirebirdPlatform;
import org.apache.ddlutils.platform.hsqldb.HsqlDbPlatform;
import org.apache.ddlutils.platform.mssql.MSSqlPlatform;
import org.apache.ddlutils.platform.mysql.MySqlPlatform;
import org.apache.ddlutils.platform.oracle.Oracle10Platform;
import org.apache.ddlutils.platform.oracle.Oracle8Platform;
import org.apache.ddlutils.platform.postgresql.PostgreSqlPlatform;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class DbDialectFactory implements FactoryBean, BeanFactoryAware {

    private static final Log logger = LogFactory.getLog(DbDialectFactory.class);

    private JdbcTemplate jdbcTemplate;

    private BeanFactory beanFactory;

    public Object getObject() throws Exception {

        waitForAvailableDatabase();

        String productName = getDbProductName();
        int majorVersion = getDbMajorVersion();

        // Try to use latest version of platform, then fallback on default
        // platform
        Platform pf = PlatformFactory.createNewPlatformInstance(productName + majorVersion);
        if (pf == null) {
            pf = PlatformFactory.createNewPlatformInstance(jdbcTemplate.getDataSource());
        } else {
            pf.setDataSource(jdbcTemplate.getDataSource());
        }

        AbstractDbDialect dialect = null;

        if (pf instanceof MySqlPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("mysqlDialect");
        } else if (pf instanceof Oracle8Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("oracleDialect");
        } else if (pf instanceof Oracle10Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("oracleDialect");
        } else if (pf instanceof MSSqlPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("msSqlDialect");
        } else if (pf instanceof PostgreSqlPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("postgresqlDialect");
        } else if (pf instanceof DerbyPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("derbyDialect");
        } else if (pf instanceof HsqlDbPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("hsqldbDialect");
        } else if (pf instanceof FirebirdPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("firebirdDialect");
        } else {
            throw new DbNotSupportedException();
        }

        dialect.init(pf);
        dialect.setTransactionTemplate((TransactionTemplate) beanFactory.getBean("currentTransactionTemplate"));
        return dialect;
    }

    private void waitForAvailableDatabase() {
        boolean success = false;
        while (!success) {
            try {
                jdbcTemplate.execute(new ConnectionCallback() {
                    public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                        return null;
                    }
                });
                success = true;
            } catch (CannotGetJdbcConnectionException ex) {
                logger.error("Could not get a connection to the database: " + ex.getMessage()
                        + ".  Waiting for 10 seconds, before trying to connect to the database again.");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private int getDbMajorVersion() {
        return (Integer) new JdbcTemplate(jdbcTemplate.getDataSource()).execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseMajorVersion();
            }
        });
    }

    private String getDbProductName() {
        return (String) new JdbcTemplate(jdbcTemplate.getDataSource()).execute(new ConnectionCallback() {
            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseProductName();
            }
        });
    }

    public Class<IDbDialect> getObjectType() {
        return IDbDialect.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

}
