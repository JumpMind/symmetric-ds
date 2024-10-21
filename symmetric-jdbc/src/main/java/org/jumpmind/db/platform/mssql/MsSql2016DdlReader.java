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
package org.jumpmind.db.platform.mssql;
 
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

/*
 * Reads a database model from a Microsoft Sql Server database.
 */
public class MsSql2016DdlReader extends MsSqlDdlReader {
    public MsSql2016DdlReader(IDatabasePlatform platform) {
        super(platform);
    }

    /**
     * Maps new native SQL Server column types VARCHARMAX and NVARCHARMAX to a platfom-independent=Types.LONGNVARCHAR
     */
    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = ((String) values.get("TYPE_NAME")).toUpperCase();
        if (typeName != null) {
            int size = -1;
            String columnSize = (String) values.get("COLUMN_SIZE");
            if (isNotBlank(columnSize)) {
                size = Integer.parseInt(columnSize);
            }
            if (typeName.contains("NVARCHAR") && size > 4000) {
                return Types.LONGNVARCHAR;
            } else if (typeName.contains("VARCHAR") && size > 8000) {
                return Types.LONGVARCHAR;
            }
        }
        return super.mapUnknownJdbcTypeForColumn(values);
    }

    final static String VARCHARMAX_SIZE = "2147483647"; // Limit is Integer.MAX_VALUE = 2147483647

    /**
     * New native SQL Server column types VARCHARMAX and NVARCHARMAX
     */
    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        String nativeTypeName = ((String) values.get(getName("TYPE_NAME"))).toUpperCase();
        String columnSize = (String) values.get(getName("COLUMN_SIZE"));
        Column column = super.readColumn(metaData, values);
        if (nativeTypeName.contains("VARCHAR") && columnSize.equals("2147483647")) {
            Map<String, PlatformColumn> platformColumnsMap = column.getPlatformColumns();
            if (nativeTypeName.equals("NVARCHAR")) {
                column.setJdbcTypeName("NVARCHARMAX");
                column.setJdbcTypeCode(ColumnTypes.MSSQL_NVARCHARMAX);
            } else {
                column.setJdbcTypeName("VARCHARMAX");
                column.setJdbcTypeCode(ColumnTypes.MSSQL_VARCHARMAX);

            }
            PlatformColumn platformColumn = platformColumnsMap.get(column.getName());
            if (platformColumn != null) {
                platformColumn.setType(column.getJdbcTypeName());
            }
            return column;
        }
        return column;
    }
}
