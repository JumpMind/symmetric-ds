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
package org.jumpmind.db.platform.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;

public class Db2zOsDdlReader extends Db2DdlReader {

    public Db2zOsDdlReader(IDatabasePlatform platform) {
        super(platform);
    }
    
    @Override
    protected void enhanceTableMetaData(Connection connection, DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
        setAutoIncrementMetaData(connection, metaData, table);
        setIsAccessControlledMetaData(connection, metaData, table);
        
    }

    /**
     * @param connection
     * @param metaData
     * @param table
     */
    protected void setIsAccessControlledMetaData(Connection connection, DatabaseMetaDataWrapper metaData, Table table)  throws SQLException {
        log.debug("about to read access control meta data.");

        String sql = "SELECT COUNT(*) AS ACCESS_CONTROLS FROM SYSIBM.SYSCONTROLS WHERE TBNAME = ?";
        if (StringUtils.isNotBlank(table.getSchema())) {
            sql = sql + " AND TBSCHEMA=?";
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, table.getName());
            if (StringUtils.isNotBlank(table.getSchema())) {
                pstmt.setString(2, table.getSchema());
            }

            rs = pstmt.executeQuery();
            if (rs.next()) {
                int accessControlCount = rs.getInt(1);
                if (accessControlCount > 0) {
                    table.setAccessControlled(true);
                    log.debug("Table {} has {} access controls.", table.getName(), accessControlCount);
                }
            }
        } finally {
            JdbcSqlTemplate.close(rs);
            JdbcSqlTemplate.close(pstmt);
        }
        log.debug("done reading access control meta data."); 
    }

    /**
     * @param connection
     * @param metaData
     * @param table
     * @throws SQLException
     */
    protected void setAutoIncrementMetaData(Connection connection, DatabaseMetaDataWrapper metaData, Table table) throws SQLException {
        log.debug("about to read additional column data");
        /* DB2 does not return the auto-increment status via the database
         metadata */
        String sql = "SELECT NAME, DEFAULT FROM SYSIBM.SYSCOLUMNS WHERE TBNAME=?";
        if (StringUtils.isNotBlank(metaData.getSchemaPattern())) {
            sql = sql + " AND TBCREATOR=?";
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, table.getName());
            if (StringUtils.isNotBlank(metaData.getSchemaPattern())) {
                pstmt.setString(2, metaData.getSchemaPattern());
            }

            rs = pstmt.executeQuery();
            while (rs.next()) {
                String columnName = rs.getString(1);
                Column column = table.getColumnWithName(columnName);
                if (column != null) {
                    String isIdentity = rs.getString(2);
                    if (isIdentity != null && 
                            (isIdentity.startsWith("I") || isIdentity.startsWith("J"))) {
                        column.setAutoIncrement(true);
                        log.debug("Found identity column {} on {}", columnName, table.getName());
                    }
                }
            }
        } finally {
            JdbcSqlTemplate.close(rs);
            JdbcSqlTemplate.close(pstmt);
        }
        log.debug("done reading additional column data");
    }
    
    public List<Trigger> getTriggers(final String catalog, final String schema,
			final String tableName) throws SqlException {
		
		List<Trigger> triggers = new ArrayList<Trigger>();

		log.debug("Reading triggers for: " + tableName);
		JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
				.getSqlTemplate();
		
		String sql = "SELECT * FROM SYSCAT.TRIGGERS "
				+ "WHERE TABNAME=? and TABSCHEMA=?";
		triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
			public Trigger mapRow(Row row) {
				Trigger trigger = new Trigger();
				trigger.setName(row.getString("TRIGNAME"));
				trigger.setSchemaName(row.getString("TRIGSCHEMA"));
				trigger.setTableName(row.getString("TABNAME"));
				trigger.setEnabled(true);
				trigger.setSource(row.getString("TEXT"));
				row.remove("TEXT");
				switch(row.getString("TRIGEVENT").charAt(0)) {
					case('I'): row.put("TRIGEVENT", "INSERT"); break;
					case('U'): row.put("TRIGEVENT", "UPDATE"); break;
					case('D'): row.put("TRIGEVENT", "DELETE");
				}
				trigger.setTriggerType(TriggerType.valueOf(row.getString("TRIGEVENT")));				
				switch(row.getString("TRIGTIME").charAt(0)) {
					case ('A'): row.put("TRIGTIME", "AFTER"); break;
					case ('B'): row.put("TRIGTIME", "BEFORE"); break;
					case ('I'): row.put("TRIGTIME", "INSTEAD OF");
				}
				trigger.setMetaData(row);
				return trigger;
			}
		}, tableName, schema);
		
		return triggers;
	}

}
