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
package org.jumpmind.driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {
    
    private static final String DRIVER_PREFIX = "jdbc:symds:";

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to register SymmetricDS driver", ex);
        }
    }    

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null || !url.toLowerCase().startsWith(DRIVER_PREFIX)) {
            return null;
        }
        
        String realUrl = getRealUrl(url);
        
        Connection connection = DriverManager.getConnection(realUrl);

        return new ConnectionWrapper(connection);
    }

    private String getRealUrl(String url) {
        // transform "jdbc:symds:jtds:" to just jdbc:jtds:
        return url.replace("symds:", "");
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }

        return url.toLowerCase().startsWith(DRIVER_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return null;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
