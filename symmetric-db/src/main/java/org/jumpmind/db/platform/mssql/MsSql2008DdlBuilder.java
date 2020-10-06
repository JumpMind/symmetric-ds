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
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.CompressionTypes;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.PlatformIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;

public class MsSql2008DdlBuilder extends MsSql2005DdlBuilder {
    
    public MsSql2008DdlBuilder() {
        this.databaseName = DatabaseNamesConstants.MSSQL2008;
    
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATE", Types.DATE);
        databaseInfo.addNativeTypeMapping(Types.TIME, "TIME", Types.TIME);
        databaseInfo.addNativeTypeMapping(ColumnTypes.MSSQL_SQL_VARIANT, "SQL_VARIANT", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME2");
        databaseInfo.addNativeTypeMapping(ColumnTypes.MAPPED_TIMESTAMPTZ, "DATETIMEOFFSET");
    }
    
    @Override
    protected void writeExternalIndexCreate(Table table, IIndex index, StringBuilder ddl) {
        super.writeExternalIndexCreate(table, index, ddl);
        if (index.getPlatformIndexes() != null && index.getPlatformIndexes().size() > 0) {
            Map<String, PlatformIndex> platformIndices = index.getPlatformIndexes();
            for(PlatformIndex platformIndex : platformIndices.values()) {
                if (StringUtils.equals(platformIndex.getName(),index.getName())) {
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
}
