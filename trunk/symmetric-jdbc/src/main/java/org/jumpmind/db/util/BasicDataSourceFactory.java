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
 * under the License. 
 */
package org.jumpmind.db.util;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.security.SecurityService;
import org.slf4j.LoggerFactory;

public class BasicDataSourceFactory {
    
    
    public static BasicDataSource create(TypedProperties properties) {
        return create(properties, new SecurityService());
    }

    public static BasicDataSource create(TypedProperties properties,
            ISecurityService securityService) {
        ResettableBasicDataSource dataSource = new ResettableBasicDataSource();
        dataSource.setDriverClassName(properties.get(
                BasicDataSourcePropertyConstants.DB_POOL_DRIVER, null));
        try {
            Driver driver = (Driver)Class.forName(dataSource.getDriverClassName()).newInstance();
            synchronized (DriverManager.class) {
                Enumeration<Driver> drivers = DriverManager.getDrivers();
                while (drivers.hasMoreElements()) {
                    Driver driver2 = (Driver) drivers.nextElement();
                    /* 
                     * MySQL and Maria DB drivers cannot co-exist because
                     * they use the same JDBC URL.
                     */
                    if ((driver.getClass().equals("com.mysql.jdbc.Driver") &&
                            driver2.getClass().getName().equals("org.mariadb.jdbc.Driver")) ||
                            (driver.getClass().equals("org.mariadb.jdbc.Driver") &&
                                    driver2.getClass().getName().equals("com.mysql.jdbc.Driver"))) {
                        DriverManager.deregisterDriver(driver2);
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Had trouble registering the jdbc driver: " + dataSource.getDriverClassName(),e);
        }
        dataSource.setUrl(properties.get(BasicDataSourcePropertyConstants.DB_POOL_URL, null));
        String user = properties.get(BasicDataSourcePropertyConstants.DB_POOL_USER, "");
        if (user != null && user.startsWith(SecurityConstants.PREFIX_ENC)) {
            user = securityService.decrypt(user.substring(SecurityConstants.PREFIX_ENC.length()));
        }
        dataSource.setUsername(user);

        String password = properties.get(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD, "");
        if (password != null && password.startsWith(SecurityConstants.PREFIX_ENC)) {
            password = securityService.decrypt(password.substring(SecurityConstants.PREFIX_ENC
                    .length()));
        }
        dataSource.setPassword(password);
        dataSource.setInitialSize(properties.getInt(
                BasicDataSourcePropertyConstants.DB_POOL_INITIAL_SIZE, 2));
        dataSource.setMaxActive(properties.getInt(
                BasicDataSourcePropertyConstants.DB_POOL_MAX_ACTIVE, 10));
        dataSource.setMaxWait(properties.getInt(BasicDataSourcePropertyConstants.DB_POOL_MAX_WAIT,
                5000));
        dataSource.setMinEvictableIdleTimeMillis(properties.getInt(
                BasicDataSourcePropertyConstants.DB_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS, 60000));
        dataSource.setTimeBetweenEvictionRunsMillis(120000);
        dataSource.setNumTestsPerEvictionRun(10);
        dataSource.setValidationQuery(properties.get(
                BasicDataSourcePropertyConstants.DB_POOL_VALIDATION_QUERY, null));
        dataSource.setTestOnBorrow(properties.is(
                BasicDataSourcePropertyConstants.DB_POOL_TEST_ON_BORROW, true));
        dataSource.setTestOnReturn(properties.is(
                BasicDataSourcePropertyConstants.DB_POOL_TEST_ON_RETURN, false));
        dataSource.setTestWhileIdle(properties.is(
                BasicDataSourcePropertyConstants.DB_POOL_TEST_WHILE_IDLE, false));

        String connectionProperties = properties.get(
                BasicDataSourcePropertyConstants.DB_POOL_CONNECTION_PROPERTIES, null);
        if (StringUtils.isNotBlank(connectionProperties)) {
            String[] tokens = connectionProperties.split(";");
            for (String property : tokens) {
                String[] keyValue = property.split("=");
                if (keyValue != null && keyValue.length > 1) {
                    LoggerFactory.getLogger(BasicDataSourceFactory.class).info(
                            "Setting database connection property {}={}", keyValue[0], keyValue[1]);
                    dataSource.addConnectionProperty(keyValue[0], keyValue[1]);
                }
            }
        }
        
        String initSql = properties.get(BasicDataSourcePropertyConstants.DB_POOL_INIT_SQL, null);
        if (StringUtils.isNotBlank(initSql)) {
            List<String> initSqlList = new ArrayList<String>(1);
            initSqlList.add(initSql);
            dataSource.setConnectionInitSqls(initSqlList);
        }
        return dataSource;

    }

}
