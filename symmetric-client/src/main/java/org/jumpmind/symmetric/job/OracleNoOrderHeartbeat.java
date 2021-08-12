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
package org.jumpmind.symmetric.job;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.security.SecurityConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.impl.ISqlMap;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class OracleNoOrderHeartbeat implements IHeartbeatListener, IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    private ISymmetricEngine engine;
    private IParameterService parameterService;
    private ISqlMap sqlMap;

    public OracleNoOrderHeartbeat(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
        sqlMap = new OracleNoOrderHeartbeatSqlMap(engine.getDatabasePlatform(), parameterService.getTablePrefix());
    }

    public void heartbeat(Node me) {
        String dbUrls = parameterService.getString(ParameterConstants.DBDIALECT_ORACLE_SEQUENCE_NOORDER_NEXTVALUE_DB_URLS);
        if (parameterService.is(ParameterConstants.HEARTBEAT_ENABLED) && StringUtils.isNotBlank(dbUrls)) {
            String user = parameterService.getString(ParameterConstants.DB_USER);
            String password = parameterService.getString(ParameterConstants.DB_PASSWORD);
            if (password != null && password.startsWith(SecurityConstants.PREFIX_ENC)) {
                try {
                    password = engine.getSecurityService().decrypt(password.substring(SecurityConstants.PREFIX_ENC.length()));
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed to decrypt the database password from the engine properties file", ex);
                }
            }
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.MILLISECOND, 0);
            String[] dbUrlArray = dbUrls.split(",");
            log.info("Connecting for heartbeat on {} RAC nodes", dbUrlArray.length);
            for (String dbUrl : dbUrlArray) {
                SingleConnectionDataSource ds = null;
                try {
                    log.debug("Connecting to {}", dbUrl);
                    Connection conn = DriverManager.getConnection(dbUrl.trim(), user, password);
                    ds = new SingleConnectionDataSource(conn, true);
                    JdbcTemplate sqlTemplate = new JdbcTemplate(ds);
                    int count = sqlTemplate.update(sqlMap.getSql("updateNodeHost"), new Timestamp(cal.getTimeInMillis()), me.getNodeId(),
                            AppUtils.getHostName());
                    log.debug("Updated {} rows for heartbeat", count);
                } catch (Exception e) {
                    log.error("Unable to update heartbeat time", e);
                } finally {
                    if (ds != null) {
                        ds.destroy();
                    }
                }
            }
        }
    }

    @Override
    public long getTimeBetweenHeartbeatsInSeconds() {
        return engine.getParameterService().getLong(ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);
    }
}
