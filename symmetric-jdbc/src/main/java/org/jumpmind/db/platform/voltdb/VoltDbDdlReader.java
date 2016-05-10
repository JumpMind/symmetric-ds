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
package org.jumpmind.db.platform.voltdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;

public class VoltDbDdlReader extends AbstractJdbcDdlReader {
    
    public final static String VOLT_DB_SYSTEM_INDEX_PREFIX = "VOLTDB_AUTOGEN_IDX_";
    
    public VoltDbDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);
    }
    
    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        return true;
    }
    
    @Override
    protected void removeSystemIndices(Connection connection, DatabaseMetaDataWrapper metaData,
            Table table) throws SQLException {
        super.removeSystemIndices(connection, metaData, table);
        
        for (int indexIdx = 0; indexIdx < table.getIndexCount();) {
            IIndex index = table.getIndex(indexIdx);

            if (index.getName() != null && index.getName().startsWith(VOLT_DB_SYSTEM_INDEX_PREFIX)) {
                table.removeIndex(indexIdx);
            } else {
                indexIdx++;
            }
        }
    }
    
    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        Table table = super.readTable(connection, metaData, values);
        disableAutoIncrement(table);
        fixIndexKeyOrder(table);
        return table;
    }

    /**
     * @param table
     */
    protected void fixIndexKeyOrder(Table table) {
        // VoltDB meta-data appears to always 
        
    }

    /**
     * @param table
     */
    protected void disableAutoIncrement(Table table) {
        for (Column column : table.getColumns()) {
            column.setAutoIncrement(false);
        } 
    }

//    @Override
//    public Database readTables(String catalog, String schema, String[] tableTypes) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public Table readTable(String catalog, String schema, String tableName) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    @Override
//    public List<String> getTableTypes() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see org.jumpmind.db.platform.IDdlReader#getCatalogNames()
//     */
//    @Override
//    public List<String> getCatalogNames() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see org.jumpmind.db.platform.IDdlReader#getSchemaNames(java.lang.String)
//     */
//    @Override
//    public List<String> getSchemaNames(String catalog) {
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see org.jumpmind.db.platform.IDdlReader#getTableNames(java.lang.String, java.lang.String, java.lang.String[])
//     */
//    @Override
//    public List<String> getTableNames(String catalog, String schema, String[] tableTypes) {
//        // TODO Auto-generated method stub
//        return null;
//    }
//
//    /* (non-Javadoc)
//     * @see org.jumpmind.db.platform.IDdlReader#getColumnNames(java.lang.String, java.lang.String, java.lang.String)
//     */
//    @Override
//    public List<String> getColumnNames(String catalog, String schema, String tableName) {
//        // TODO Auto-generated method stub
//        return null;
//    }

}
