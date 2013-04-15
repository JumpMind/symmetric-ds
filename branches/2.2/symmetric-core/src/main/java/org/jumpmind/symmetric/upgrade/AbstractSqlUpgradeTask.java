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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 */
abstract public class AbstractSqlUpgradeTask implements IUpgradeTask {

    protected JdbcTemplate jdbcTemplate;

    protected boolean isUpgradeRegistrationServer = true;

    protected boolean isUpgradeNonRegistrationServer = true;

    protected boolean useReplacement = true;

    public void upgrade(int[] fromVersion) {
    }

    protected String prepareSql(String nodeId, IParameterService parameterService, String sql) {
        if (useReplacement) {
            sql = replace("groupId", parameterService.getNodeGroupId(), sql);
            sql = replace("externalId", parameterService.getExternalId(), sql);
            sql = replace("nodeId", nodeId, sql);
        }
        return sql;
    }

    protected String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    public void setJdbcTemplate(JdbcTemplate jdbc) {
        this.jdbcTemplate = jdbc;
    }

    public boolean isUpgradeNonRegistrationServer() {
        return isUpgradeNonRegistrationServer;
    }

    public boolean getUpgradeNonRegistrationServer() {
        return isUpgradeNonRegistrationServer;
    }

    public void setUpgradeNonRegistrationServer(boolean isUpgradeNonRegistrationServer) {
        this.isUpgradeNonRegistrationServer = isUpgradeNonRegistrationServer;
    }

    public boolean isUpgradeRegistrationServer() {
        return isUpgradeRegistrationServer;
    }

    public boolean getUpgradeRegistrationServer() {
        return isUpgradeRegistrationServer;
    }

    public void setUpgradeRegistrationServer(boolean isUpgradeRegistrationServer) {
        this.isUpgradeRegistrationServer = isUpgradeRegistrationServer;
    }

    public boolean getUseReplacement() {
        return useReplacement;
    }

    public void setUseReplacement(boolean useReplacement) {
        this.useReplacement = useReplacement;
    }

}