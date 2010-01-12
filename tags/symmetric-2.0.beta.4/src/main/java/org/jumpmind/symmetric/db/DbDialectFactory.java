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
import java.util.Map;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.platform.db2.Db2Platform;
import org.apache.ddlutils.platform.derby.DerbyPlatform;
import org.apache.ddlutils.platform.firebird.FirebirdPlatform;
import org.apache.ddlutils.platform.hsqldb.HsqlDbPlatform;
import org.apache.ddlutils.platform.mssql.MSSqlPlatform;
import org.apache.ddlutils.platform.mysql.MySqlPlatform;
import org.apache.ddlutils.platform.oracle.Oracle10Platform;
import org.apache.ddlutils.platform.oracle.Oracle8Platform;
import org.apache.ddlutils.platform.oracle.Oracle9Platform;
import org.apache.ddlutils.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.h2.H2Platform;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class DbDialectFactory implements FactoryBean<IDbDialect>, BeanFactoryAware {

    private static final ILog log = LogFactory.getLog(DbDialectFactory.class);

    private String db2zSeriesProductVersion;

    private JdbcTemplate jdbcTemplate;

    private BeanFactory beanFactory;

    private Map<String, Class<Platform>> customPlatforms;

    public IDbDialect getObject() throws Exception {

        waitForAvailableDatabase();

        String productName = getDbProductName();
        int majorVersion = getDbMajorVersion();

        // Try to use latest version of platform, then fallback on default
        // platform
        String productString = productName;
        if (majorVersion > 0) {
            productString += majorVersion;
        }
        if (productName.startsWith("DB2")) {
            productString = "DB2v8";
        }

        String currentDbProductVersion = getDatabaseProductVersion();
        initPlatforms();

        Platform pf = PlatformFactory.createNewPlatformInstance(productString);
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
        } else if (pf instanceof Oracle9Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("oracleDialect");
        } else if (pf instanceof Oracle10Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("oracleDialect");
        } else if (pf instanceof MSSqlPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("msSqlDialect");
        } else if (pf instanceof PostgreSqlPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("postgresqlDialect");
        } else if (pf instanceof DerbyPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("derbyDialect");
        } else if (pf instanceof H2Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("h2Dialect");
        } else if (pf instanceof HsqlDbPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("hsqldbDialect");
        } else if (pf instanceof Db2Platform) {
            if (currentDbProductVersion.equals(db2zSeriesProductVersion)) {
                dialect = (AbstractDbDialect) beanFactory.getBean("db2zSeriesDialect");
            } else {
                dialect = (AbstractDbDialect) beanFactory.getBean("db2Dialect");
            }
        } else if (pf instanceof FirebirdPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("firebirdDialect");
        } else {
            throw new DbNotSupportedException();
        }

        dialect.init(pf);
        dialect.setTransactionTemplate((TransactionTemplate) beanFactory.getBean("currentTransactionTemplate"));
        return dialect;
    }

    private void initPlatforms() throws IllegalStateException {
        if (customPlatforms != null) {
            for (Map.Entry<String, Class<Platform>> entry : customPlatforms.entrySet()) {
                if (!Platform.class.isAssignableFrom(entry.getValue())) {
                    throw new IllegalStateException("Platform is not valid:" + entry.getValue());
                }
                PlatformFactory.registerPlatform(entry.getKey(), entry.getValue());
            }
        }
    }

    private void waitForAvailableDatabase() {
        boolean success = false;
        while (!success) {
            try {
                jdbcTemplate.execute(new ConnectionCallback<Object>() {
                    public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                        return null;
                    }
                });
                success = true;
            } catch (CannotGetJdbcConnectionException ex) {
                log.error("DatabaseConnectionException", ex.getMessage());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private int getDbMajorVersion() {
        return new JdbcTemplate(jdbcTemplate.getDataSource()).execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                try {
                    return metaData.getDatabaseMajorVersion();
                } catch (UnsupportedOperationException e) {
                    return 0;
                }
            }
        });
    }

    private String getDatabaseProductVersion() {
        return new JdbcTemplate(jdbcTemplate.getDataSource()).execute(new ConnectionCallback<String>() {
            public String doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseProductVersion();
            }
        });
    }

    private String getDbProductName() {
        return new JdbcTemplate(jdbcTemplate.getDataSource()).execute(new ConnectionCallback<String>() {
            public String doInConnection(Connection c) throws SQLException, DataAccessException {
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

    public void setCustomPlatforms(Map<String, Class<Platform>> customPlatforms) {
        this.customPlatforms = customPlatforms;
    }

    /**
     * Sets the database product version for zOS db2 from the properties file
     */
    public void setDb2zSeriesProductVersion(String version) {
        this.db2zSeriesProductVersion = version;
    }

}
