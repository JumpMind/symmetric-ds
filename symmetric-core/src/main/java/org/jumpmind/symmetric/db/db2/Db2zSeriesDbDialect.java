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
package org.jumpmind.symmetric.db.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class Db2zSeriesDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(Db2zSeriesDbDialect.class);

    private String userName;

    /*
     * Returns the database user id
     * 
     * @return String
     */
    public String getUserName() {
        return userName;
    }

    /*
     * Sets the database user id from properties file
     * 
     * @param userName
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM SYSIBM.SYSTRIGGERS WHERE NAME = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "nullif('','')";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
    }

    public boolean isNonBlankCharColumnSpacePadded() {
        return true;
    }

    public boolean isCharColumnSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
    }

    public String getSyncTriggersExpression() {
        return "";
    }
    
    @Override
    public long insertWithGeneratedKey(final String sql, final SequenceIdentifier sequenceId,
            final PreparedStatementCallback<Object> callback) {
        return (Long) jdbcTemplate.execute(new ConnectionCallback<Long>() {
            public Long doInConnection(Connection conn) throws SQLException, DataAccessException {

                long key = 0;
                PreparedStatement ps = null;
                try {
                    boolean supportsGetGeneratedKeys = supportsGetGeneratedKeys();
                    boolean supportsReturningKeys = supportsReturningKeys();
                    if (allowsNullForIdentityColumn()) {
                        if (supportsGetGeneratedKeys) {
                            ps = conn.prepareStatement(sql, new int[] { 1 });
                        } else if (supportsReturningKeys) {
                            ps = conn.prepareStatement(sql + " returning " + getSequenceKeyName(sequenceId));
                        } else {
                            ps = conn.prepareStatement(sql);
                        }
                    } else {
                        String replaceSql = sql.replaceFirst("\\(\\w*,", "(").replaceFirst("\\(null,", "(");
                        System.out.println("======================================");
                        System.out.println(replaceSql);
                        System.out.println("======================================");
                        if (supportsGetGeneratedKeys) {
                            ps = conn.prepareStatement(replaceSql, Statement.RETURN_GENERATED_KEYS);
                        } else {
                            ps = conn.prepareStatement(replaceSql);
                        }
                    }
                    ps.setQueryTimeout(jdbcTemplate.getQueryTimeout());
                    if (callback != null) {
                        callback.doInPreparedStatement(ps);
                    }

                    ResultSet rs = null;
                    if (supportsGetGeneratedKeys) {
                        ps.executeUpdate();
                        try {
                            rs = ps.getGeneratedKeys();
                            if (rs.next()) {
                                key = rs.getLong(1);
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                        }
                    } else if (supportsReturningKeys) {
                        try {
                            rs = ps.executeQuery();
                            if (rs.next()) {
                                key = rs.getLong(1);
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                        }
                    } else {
                        Statement st = null;
                        ps.executeUpdate();
                        try {
                            st = conn.createStatement();
                            rs = st.executeQuery(getSelectLastInsertIdSql(getSequenceName(sequenceId)));
                            if (rs.next()) {
                                key = rs.getLong(1);
                            }
                        } finally {
                            JdbcUtils.closeResultSet(rs);
                            JdbcUtils.closeStatement(st);
                        }
                    }
                } finally {
                    JdbcUtils.closeStatement(ps);
                }
                return key;
            }
        });
    }
}