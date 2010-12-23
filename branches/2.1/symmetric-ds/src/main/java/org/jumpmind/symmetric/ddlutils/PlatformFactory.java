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
package org.jumpmind.symmetric.ddlutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.platform.mssql.MSSqlPlatform;
import org.jumpmind.symmetric.ddlutils.oracle.OraclePlatform;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

public class PlatformFactory {

    final static ILog log = LogFactory.getLog(PlatformFactory.class);
    
    static private boolean initialized = false;

    public static Platform getPlatform(DataSource dataSource) {
        initPlatforms();
        String productName = getDbProductName(dataSource);
        int majorVersion = getDbMajorVersion(dataSource);

        // Try to use latest version of platform, then fallback on default
        // platform
        String productString = productName;
        if (majorVersion > 0) {
            productString += majorVersion;
        }
        
        if (productName.startsWith("DB2")) {
            productString = "DB2v8";
        }

        Platform pf = org.jumpmind.symmetric.ddl.PlatformFactory.createNewPlatformInstance(productString);

        if (pf == null) {
            pf = org.jumpmind.symmetric.ddl.PlatformFactory.createNewPlatformInstance(dataSource);
        } else {
            pf.setDataSource(dataSource);
        }
        
        if (pf instanceof MSSqlPlatform && !pf.isDelimitedIdentifierModeOn()) {
            log.info("PlatformTurningOnDelimitedIdentifierMode");
            pf.setDelimitedIdentifierModeOn(true);
        }
        
        log.info("PlatformInUse", pf.getClass().getName());

        return pf;
    }

    public static String getDbProductName(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<String>() {
            public String doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseProductName();
            }
        });
    }

    public static String getDatabaseProductVersion(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<String>() {
            public String doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                return metaData.getDatabaseProductVersion();
            }
        });
    }

    public static int getDbMajorVersion(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<Integer>() {
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
    
    public static int getDbMinorVersion(DataSource dataSource) {
        return new JdbcTemplate(dataSource).execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = c.getMetaData();
                try {
                    return metaData.getDatabaseMinorVersion();
                } catch (UnsupportedOperationException e) {
                    return 0;
                }
            }
        });
    }

    private synchronized static void initPlatforms() {
        if (!initialized) {
            org.jumpmind.symmetric.ddl.PlatformFactory.registerPlatform(OraclePlatform.DATABASENAME, 
                    OraclePlatform.class);
            initialized = true;
        }
    }

}