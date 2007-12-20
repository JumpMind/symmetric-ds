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

import javax.sql.DataSource;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.platform.mysql.MySqlPlatform;
import org.apache.ddlutils.platform.oracle.Oracle10Platform;
import org.apache.ddlutils.platform.oracle.Oracle8Platform;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

public class DbDialectFactory implements FactoryBean, BeanFactoryAware {

    private DataSource dataSource;

    private BeanFactory beanFactory;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Object getObject() throws Exception {
        AbstractDbDialect dialect = null;

        String productName = getDbProductName();
        int majorVersion = getDbMajorVersion();
        Platform pf = PlatformFactory.createNewPlatformInstance(productName
                + majorVersion);
        pf.setDataSource(dataSource);

        if (pf instanceof MySqlPlatform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("mysqlDialect");
        } else if (pf instanceof Oracle8Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("oracleDialect");
        } else if (pf instanceof Oracle10Platform) {
            dialect = (AbstractDbDialect) beanFactory.getBean("oracleDialect");
        } else {
            throw new DbNotSupportedException();

        }

        dialect.init(pf);

        return dialect;
    }

    private int getDbMajorVersion() {
        return (Integer) new JdbcTemplate(dataSource)
                .execute(new ConnectionCallback() {
                    public Object doInConnection(Connection c)
                            throws SQLException, DataAccessException {
                        DatabaseMetaData metaData = c.getMetaData();
                        return metaData.getDatabaseMajorVersion();
                    }
                });
    }

    private String getDbProductName() {
        return (String) new JdbcTemplate(dataSource)
                .execute(new ConnectionCallback() {
                    public Object doInConnection(Connection c)
                            throws SQLException, DataAccessException {
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

}
