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


package org.jumpmind.symmetric.upgrade;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.core.RowCallbackHandler;

/**
 * 
 */
public class SqlDrivenUpgradeTask extends AbstractSqlUpgradeTask {

    private static final ILog log = LogFactory.getLog(SqlDrivenUpgradeTask.class);

    protected String driverSql;

    protected String updateSql;

    public void upgrade(final String nodeId, final IParameterService parameterService, int[] fromVersion) {
        String sql = prepareSql(nodeId, parameterService, driverSql);
        log.warn("SqlForEachUpgrade", sql);
        log.warn("SqlDoUpgrade", updateSql);
        jdbcTemplate.query(sql, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                int count = rs.getMetaData().getColumnCount();
                Object[] params = new Object[count];
                for (int i = 0; i < count; i++) {
                    params[i] = rs.getObject(i + 1);
                }
                jdbcTemplate.update(prepareSql(nodeId, parameterService, updateSql), params);
            }
        });
    }

    public void setDriverSql(String driverSql) {
        this.driverSql = driverSql;
    }

    public void setUpdateSql(String updateSql) {
        this.updateSql = updateSql;
    }

}