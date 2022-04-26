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

import java.sql.Types;
import java.util.Map;

import org.apache.commons.codec.binary.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.CompressionTypes;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.PlatformIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MsSql2008DdlBuilder extends MsSql2005DdlBuilder {
    public static final String CHANGE_TRACKING_SYM_PREFIX = "SymmetricDS";

    public MsSql2008DdlBuilder() {
        super();
        this.databaseName = DatabaseNamesConstants.MSSQL2008;
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATE", Types.DATE);
        databaseInfo.addNativeTypeMapping(Types.TIME, "TIME", Types.TIME);
        databaseInfo.addNativeTypeMapping(ColumnTypes.MSSQL_SQL_VARIANT, "SQL_VARIANT", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME2");
        databaseInfo.addNativeTypeMapping(ColumnTypes.TIMESTAMPTZ, "DATETIMEOFFSET");
        databaseInfo.addNativeTypeMapping(ColumnTypes.TIMESTAMPLTZ, "DATETIMEOFFSET", ColumnTypes.TIMESTAMPTZ);
        databaseInfo.setHasSize(Types.TIMESTAMP, true);
        databaseInfo.setHasSize(ColumnTypes.TIMESTAMPTZ, true);
        databaseInfo.setHasSize(ColumnTypes.TIMESTAMPLTZ, true);
        databaseInfo.setHasSize(Types.TIME, true);
        databaseInfo.setHasSize(ColumnTypes.TIMETZ, true);
        databaseInfo.setDefaultSize(Types.TIMESTAMP, 7);
        databaseInfo.setDefaultSize(Types.TIME, 7);
        databaseInfo.setDefaultSize(ColumnTypes.TIMESTAMPTZ, 7);
        databaseInfo.setMaxSize("DATETIME2", 7);
        databaseInfo.setMaxSize("TIME", 7);
        databaseInfo.setMaxSize("DATETIMEOFFSET", 7);
    }

    @Override
    protected boolean hasSize(Column column) {
        if (column.getMappedTypeCode() == Types.TIMESTAMP) {
            PlatformColumn platformColumn = column.findPlatformColumn(databaseName);
            String nativeType = getNativeType(column);
            if (platformColumn != null) {
                nativeType = platformColumn.getType();
            }
            if (nativeType.equalsIgnoreCase("DATETIME") || nativeType.equalsIgnoreCase("SMALLDATETIME")) {
                return false;
            }
        }
        return super.hasSize(column);
    }

    @Override
    protected void writeExternalIndexCreate(Table table, IIndex index, StringBuilder ddl) {
        super.writeExternalIndexCreate(table, index, ddl);
        if (index.getPlatformIndexes() != null && index.getPlatformIndexes().size() > 0) {
            Map<String, PlatformIndex> platformIndices = index.getPlatformIndexes();
            for (PlatformIndex platformIndex : platformIndices.values()) {
                if (StringUtils.equals(platformIndex.getName(), index.getName())) {
                    if (platformIndex.getFilterCondition() != null) {
                        println(ddl);
                        ddl.append(platformIndex.getFilterCondition());
                        println(ddl);
                    }
                    if (platformIndex.getCompressionType() != CompressionTypes.NONE) {
                        println(ddl);
                        ddl.append(" WITH(data_compression=" + platformIndex.getCompressionType().name() + ")");
                        println(ddl);
                    }
                }
            }
        }
    }

    @Override
    protected void writeTableCreationStmt(Table table, StringBuilder ddl) {
        super.writeTableCreationStmt(table, ddl);
        if (table.getCompressionType() != CompressionTypes.NONE) {
            ddl.append(" WITH(data_compression=" + table.getCompressionType().name() + ")");
        }
    }

    @Override
    public void initCteExpression() {
        if (getDatabaseInfo().isLogBased()) {
            getDatabaseInfo().setCteExpression("DECLARE @ctcontext varbinary(128) = CAST('" + CHANGE_TRACKING_SYM_PREFIX
                    + ":' AS varbinary(128)); WITH CHANGE_TRACKING_CONTEXT (@ctcontext) ");
        }
    }
}
