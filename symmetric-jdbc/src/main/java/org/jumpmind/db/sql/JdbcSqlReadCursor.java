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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSqlReadCursor<T> implements ISqlReadCursor<T> {
    
    static final Logger log = LoggerFactory.getLogger(JdbcSqlReadCursor.class);
    
    protected Connection c;

    protected ResultSet rs;

    protected Statement st;

    protected boolean autoCommitFlag;

    protected ISqlRowMapper<T> mapper;

    protected JdbcSqlTemplate sqlTemplate;

    protected int rowNumber;
    
    protected int originalIsolationLevel;

    public JdbcSqlReadCursor() {
    }

    public JdbcSqlReadCursor(JdbcSqlTemplate sqlTemplate, ISqlRowMapper<T> mapper, String sql,
            Object[] values, int[] types) {
        this.sqlTemplate = sqlTemplate;
        this.mapper = mapper;
        try {
            c = sqlTemplate.getDataSource().getConnection();
        	originalIsolationLevel = c.getTransactionIsolation();            
            autoCommitFlag = c.getAutoCommit();
            if (c.getTransactionIsolation() != sqlTemplate.getIsolationLevel()) {
            	c.setTransactionIsolation(sqlTemplate.getIsolationLevel());
            }
            if (sqlTemplate.isRequiresAutoCommitFalseToSetFetchSize()) {
                c.setAutoCommit(false);
            }

            try {
                if (values != null) {
                    PreparedStatement pstmt = c.prepareStatement(sql,
                            sqlTemplate.getSettings().getResultSetType(),
                            java.sql.ResultSet.CONCUR_READ_ONLY);
                    sqlTemplate.setValues(pstmt, values, types, sqlTemplate.getLobHandler()
                            .getDefaultHandler());
                    st = pstmt;                    
                    st.setQueryTimeout(sqlTemplate.getSettings().getQueryTimeout());
                    st.setFetchSize(sqlTemplate.getSettings().getFetchSize());
                    rs = pstmt.executeQuery();

                } else {
                    st = c.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY);
                    st.setQueryTimeout(sqlTemplate.getSettings().getQueryTimeout());
                    st.setFetchSize(sqlTemplate.getSettings().getFetchSize());
                    rs = st.executeQuery(sql);
                }
            } catch (SQLException e) {
                /*
                 * The Xerial SQLite JDBC driver throws an exception if a query
                 * returns an empty set This gets around that
                 */
                if (e.getMessage() == null
                        || !e.getMessage().equalsIgnoreCase("query does not return results")) {
                    throw e;
                }
            }
            SqlUtils.addSqlReadCursor(this);
            
        } catch (SQLException ex) {
            close();
            throw sqlTemplate.translate("Failed to execute sql: " + sql, ex);
        } catch (Throwable ex) {
            close();
            throw sqlTemplate.translate("Failed to execute sql: " + sql, ex);
        }
    }

    public T next() {
        try {
            while (rs!=null && rs.next()) {
                Row row = getMapForRow(rs, sqlTemplate.getSettings().isReadStringsAsBytes());
                T value = mapper.mapRow(row);
                if (value != null) {
                    return value;
                }
            } 
            return null;
        } catch (SQLException ex) {
            throw sqlTemplate.translate(ex);
        }
    }

    protected static Row getMapForRow(ResultSet rs, boolean readStringsAsBytes) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Row mapOfColValues = new Row(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String key = JdbcSqlTemplate.lookupColumnName(rsmd, i);
            Object obj = JdbcSqlTemplate.getResultSetValue(rs, i, readStringsAsBytes);
            mapOfColValues.put(key, obj);
        }
        return mapOfColValues;
    }

	public void close() {
		JdbcSqlTemplate.close(rs);
		JdbcSqlTemplate.close(st);
		JdbcSqlTemplate.close(autoCommitFlag, originalIsolationLevel, c);
		SqlUtils.removeSqlReadCursor(this);
	}
}
