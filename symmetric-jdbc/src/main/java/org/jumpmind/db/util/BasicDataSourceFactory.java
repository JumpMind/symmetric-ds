/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import java.math.BigInteger;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.slf4j.LoggerFactory;

public class BasicDataSourceFactory {
    
    public static void prepareDriver(String clazzName) throws Exception {
        Driver driver = (Driver)Class.forName(clazzName).newInstance();
        synchronized (DriverManager.class) {
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            while (drivers.hasMoreElements()) {
                Driver driver2 = (Driver) drivers.nextElement();                    
                /* 
                 * MySQL and Maria DB drivers cannot co-exist because
                 * they use the same JDBC URL.
                 */
                if ((driver.getClass().getName().equals("com.mysql.jdbc.Driver") &&
                        driver2.getClass().getName().equals("org.mariadb.jdbc.Driver")) ||
                        (driver.getClass().getName().equals("org.mariadb.jdbc.Driver") &&
                                driver2.getClass().getName().equals("com.mysql.jdbc.Driver"))) {
                    DriverManager.deregisterDriver(driver2);
                }
            }
        }
    }
    
    public static ResettableBasicDataSource create(TypedProperties properties) {
        return create(properties, SecurityServiceFactory.create(SecurityServiceType.CLIENT, properties));
    }

    public static ResettableBasicDataSource create(TypedProperties properties,
            ISecurityService securityService) {
        properties = properties.copy();
        properties.putAll(System.getProperties());
        ResettableBasicDataSource dataSource = new ResettableBasicDataSource();
        dataSource.setDriverClassName(properties.get(
                BasicDataSourcePropertyConstants.DB_POOL_DRIVER, null));
        try {
            prepareDriver(dataSource.getDriverClassName());
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
        dataSource.setMaxIdle(properties.getInt(BasicDataSourcePropertyConstants.DB_POOL_MAX_IDLE,
                8));
        dataSource.setMinIdle(properties.getInt(BasicDataSourcePropertyConstants.DB_POOL_MIN_IDLE,
                0));                
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
        
        List<String> initSqlList = new ArrayList<String>(3);
        
        
        if (dataSource.getDriverClassName().equals("org.sqlite.JDBC") && !password.isEmpty()) {
            initSqlList.add(String.format("pragma hexkey = '%x'", new BigInteger(1, password.getBytes())));
            initSqlList.add("select 1 from sqlite_master");
        }
        
        
        String initSql = properties.get(BasicDataSourcePropertyConstants.DB_POOL_INIT_SQL, null);
        if (StringUtils.isNotBlank(initSql)){
        	initSqlList.add(initSql);
        }
        
        if(!initSqlList.isEmpty()){
        	dataSource.setConnectionInitSqls(initSqlList);
        }
        
        return dataSource;

    }

}
