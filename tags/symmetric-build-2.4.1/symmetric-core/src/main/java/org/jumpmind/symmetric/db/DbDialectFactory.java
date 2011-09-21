/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.symmetric.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.PlatformFactory;
import org.jumpmind.symmetric.ddl.PlatformUtils;
import org.jumpmind.symmetric.ddl.platform.db2.Db2Platform;
import org.jumpmind.symmetric.ddl.platform.derby.DerbyPlatform;
import org.jumpmind.symmetric.ddl.platform.firebird.FirebirdPlatform;
import org.jumpmind.symmetric.ddl.platform.h2.H2Platform;
import org.jumpmind.symmetric.ddl.platform.hsqldb.HsqlDbPlatform;
import org.jumpmind.symmetric.ddl.platform.hsqldb2.HsqlDb2Platform;
import org.jumpmind.symmetric.ddl.platform.informix.InformixPlatform;
import org.jumpmind.symmetric.ddl.platform.interbase.InterbasePlatform;
import org.jumpmind.symmetric.ddl.platform.mssql.MSSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.mysql.MySqlPlatform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle10Platform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle8Platform;
import org.jumpmind.symmetric.ddl.platform.oracle.Oracle9Platform;
import org.jumpmind.symmetric.ddl.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.sqlite.SqLitePlatform;
import org.jumpmind.symmetric.ddl.platform.sybase.SybasePlatform;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 */
public class DbDialectFactory implements FactoryBean<IDbDialect>, BeanFactoryAware {

    private static final ILog log = LogFactory.getLog(DbDialectFactory.class);

    private String db2zSeriesProductVersion;

    private JdbcTemplate jdbcTemplate;

    private BeanFactory beanFactory;
    
    private int queryTimeout;
    
    private boolean forceDelimitedIdentifierModeOn = false;
    
    private boolean forceDelimitedIdentifierModeOff = false;

    public IDbDialect getObject() throws Exception {

        waitForAvailableDatabase();

        Platform pf = PlatformFactory.createNewPlatformInstance(jdbcTemplate.getDataSource());
        
        if (forceDelimitedIdentifierModeOn) {
            pf.setDelimitedIdentifierModeOn(true);
        }
        
        if (forceDelimitedIdentifierModeOff) {
            pf.setDelimitedIdentifierModeOn(false);
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
        } else if (pf instanceof SqLitePlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("sqliteDialect");
        } else if (pf instanceof HsqlDbPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("hsqldbDialect");
        } else if (pf instanceof HsqlDb2Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("hsqldb2Dialect");            
        } else if (pf instanceof InformixPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("informixDialect");
        } else if (pf instanceof Db2Platform) {
            String currentDbProductVersion = PlatformUtils.getDatabaseProductVersion(jdbcTemplate
                    .getDataSource());
            if (currentDbProductVersion.equals(db2zSeriesProductVersion)) {
                dialect = (AbstractDbDialect) beanFactory.getBean("db2zSeriesDialect");
            } else {
                int dbMajorVersion = PlatformUtils.getDatabaseMajorVersion(jdbcTemplate.getDataSource());
                int dbMinorVersion = PlatformUtils.getDatabaseMinorVersion(jdbcTemplate.getDataSource());
                if (dbMajorVersion < 9 || (dbMajorVersion == 9 && dbMinorVersion < 5)) {
                    dialect = (AbstractDbDialect) beanFactory.getBean("db2Dialect");
                } else {
                    dialect = (AbstractDbDialect) beanFactory.getBean("db2v9Dialect");
                }
            }
        } else if (pf instanceof FirebirdPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("firebirdDialect");
        } else if (pf instanceof SybasePlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("sybaseDialect");
        } else if (pf instanceof InterbasePlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("interbaseDialect");
        } else {
            throw new DbNotSupportedException();
        }

        dialect.init(pf, queryTimeout, jdbcTemplate);
        return dialect;
    }

    private void waitForAvailableDatabase() {
        boolean success = false;
        while (!success) {
            try {
                jdbcTemplate.execute(new ConnectionCallback<Object>() {
                    public Object doInConnection(Connection con) throws SQLException,
                            DataAccessException {
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

    /**
     * Sets the database product version for zOS db2 from the properties file
     */
    public void setDb2zSeriesProductVersion(String version) {
        this.db2zSeriesProductVersion = version;
    }
    
    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }
    
    public void setForceDelimitedIdentifierModeOn(boolean forceDelimitedIdentifierModeOn) {
        this.forceDelimitedIdentifierModeOn = forceDelimitedIdentifierModeOn;
    }
    
    public void setForceDelimitedIdentifierModeOff(boolean forceDelimitedIdentifierModeOff) {
        this.forceDelimitedIdentifierModeOff = forceDelimitedIdentifierModeOff;
    }
}