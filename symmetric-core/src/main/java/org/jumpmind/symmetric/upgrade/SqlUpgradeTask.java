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

import java.util.List;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * 
 */
public class SqlUpgradeTask extends AbstractSqlUpgradeTask {

    private static final ILog log = LogFactory.getLog(SqlUpgradeTask.class);

    protected IDbDialect dbDialect;

    protected String dialectName;

    protected List<String> sqlList;

    protected boolean ignoreFailure;

    public void upgrade(int[] fromVersion) {
        for (String sql : sqlList) {
            log.warn("SqlUpgrade", sql);
            jdbcTemplate.update(sql);
        }
    }

    public void upgrade(String nodeId, IParameterService parameterService, int[] fromVersion) {
        if (dialectName == null || (dbDialect != null && dbDialect.getName().equalsIgnoreCase((dialectName)))) {
            for (String sql : sqlList) {
                sql = prepareSql(nodeId, parameterService, sql);
                log.warn("SqlUpgrade", sql);
                if (ignoreFailure) {
                    try {
                        jdbcTemplate.update(sql);
                    } catch (Exception e) {
                        log.warn("SqlUpgradeIgnoring", e.getMessage());
                    }
                } else {
                    jdbcTemplate.update(sql);
                }
            }
        }
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }

    public void setIgnoreFailure(boolean ignoreFailure) {
        this.ignoreFailure = ignoreFailure;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setDialectName(String dialectName) {
        this.dialectName = dialectName;
    }

}