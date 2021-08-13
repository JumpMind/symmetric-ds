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
package org.jumpmind.db.sql;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeCatalogConnectionHandler implements IConnectionHandler {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    private String previousCatalog;
    private String changeCatalog;

    public ChangeCatalogConnectionHandler(String newCatalog) {
        changeCatalog = newCatalog;
    }

    @Override
    public void before(Connection connection) {
        if (changeCatalog != null) {
            try {
                previousCatalog = connection.getCatalog();
                connection.setCatalog(changeCatalog);
            } catch (SQLException e) {
                log.debug("Unable to switch to catalog '{}': ", changeCatalog, e.getMessage());
                if (changeCatalog != null) {
                    try {
                        connection.setCatalog(previousCatalog);
                    } catch (SQLException ex) {
                    }
                }
                throw new SqlException(e);
            }
        }
    }

    @Override
    public void after(Connection connection) {
        try {
            if (previousCatalog != null) {
                connection.setCatalog(previousCatalog);
            }
        } catch (SQLException ex) {
        }
    }
}
