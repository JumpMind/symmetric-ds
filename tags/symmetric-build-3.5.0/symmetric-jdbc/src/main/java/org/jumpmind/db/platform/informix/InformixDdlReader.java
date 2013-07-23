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
package org.jumpmind.db.platform.informix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

public class InformixDdlReader extends AbstractJdbcDdlReader {

    public InformixDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
    }

    @Override
    public Collection<IIndex> readIndices(Connection connection, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        String sql = "select rtrim(dbinfo('dbname')) as TABLE_CAT, st.owner as TABLE_SCHEM, st.tabname as TABLE_NAME, "
                + "case when idxtype = 'U' then 0 else 1 end NON_UNIQUE, si.owner as INDEX_QUALIFIER, si.idxname as INDEX_NAME,  "
                + "3 as TYPE,  "
                + "case when sc.colno = si.part1 then 1 "
                + "when sc.colno = si.part1 then 1 "
                + "when sc.colno = si.part2 then 2 "
                + "when sc.colno = si.part3 then 3 "
                + "when sc.colno = si.part4 then 4 "
                + "when sc.colno = si.part5 then 5 "
                + "when sc.colno = si.part6 then 6 "
                + "when sc.colno = si.part7 then 7 "
                + "when sc.colno = si.part8 then 8 "
                + "else 0 end as ORDINAL_POSITION,  "
                + "sc.colname as COLUMN_NAME, "
                + "null::varchar as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null::varchar as FILTER_CONDITION "
                + "from sysindexes si "
                + "inner join systables st on si.tabid = st.tabid "
                + "inner join syscolumns sc on si.tabid = sc.tabid "
                + "where st.tabname like ? "
                + "and (sc.colno = si.part1 or sc.colno = si.part2 or sc.colno = si.part3 or  "
                + "sc.colno = si.part4 or sc.colno = si.part5 or sc.colno = si.part6 or  "
                + "sc.colno = si.part7 or sc.colno = si.part8) and " 
                + "si.idxname not in (select idxname from sysconstraints where constrtype in ('R'))";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, tableName);

        ResultSet rs = ps.executeQuery();

        Map indices = new ListOrderedMap();
        while (rs.next()) {
            Map values = readMetaData(rs, getColumnsForIndex());
            readIndex(metaData, values, indices);
        }

        rs.close();
        ps.close();
        return indices.values();
    }
    
    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        if ("SERIAL".equalsIgnoreCase(column.getJdbcTypeName()) || "BIGSERIAL".equalsIgnoreCase(column.getJdbcTypeName())) {
            column.setAutoIncrement(true);
        }
        return column;
    }

    @Override
    public void removeSystemIndices(Connection connection, DatabaseMetaDataWrapper metaData,
            Table table) throws SQLException {
        super.removeSystemIndices(connection, metaData, table);
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        return index.getName().startsWith(" ");
    }

    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index1)
            throws SQLException {
        return fk.getName().startsWith(" ");
    }
}
