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
package org.jumpmind.db.platform;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

abstract public class AbstractJdbcDatabasePlatform extends AbstractDatabasePlatform {

    protected DataSource dataSource;

    protected ISqlTemplate sqlTemplate;

    protected SqlTemplateSettings settings;

    public AbstractJdbcDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        this.dataSource = dataSource;
        this.settings = settings;
        this.ddlBuilder = createDdlBuilder();
        this.sqlTemplate = createSqlTemplate();
        this.ddlReader = createDdlReader();
    }

    protected abstract IDdlBuilder createDdlBuilder();
    
    protected abstract IDdlReader createDdlReader();
    
    protected ISqlTemplate createSqlTemplate() {
        return new JdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo()); 
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataSource() {
        return (T) dataSource;
    }

    public void resetDataSource() {
        if (dataSource instanceof BasicDataSource) {
            BasicDataSource dbcp = (BasicDataSource) dataSource;
            try {
                dbcp.close();
            } catch (SQLException e) {
                throw sqlTemplate.translate(e);
            }
        }
    }

}
