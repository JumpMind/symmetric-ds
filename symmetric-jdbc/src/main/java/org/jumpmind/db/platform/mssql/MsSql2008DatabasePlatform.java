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

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the Microsoft SQL Server 2008 database.
 */
public class MsSql2008DatabasePlatform extends MsSql2005DatabasePlatform {

    /*
     * Creates a new platform instance.
     */
    public MsSql2008DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }
    
    @Override
    protected IDdlBuilder createDdlBuilder() {
        return new MsSql2008DdlBuilder();
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.MSSQL2008;
    }
    
    @Override
    public long getEstimatedRowCount(Table table) {
        String catalog = StringUtils.isNotBlank(table.getCatalog()) ? table.getCatalog() + "." : "";
        return getSqlTemplateDirty().queryForLong("select sum(p.rows) from " + catalog + "sys.tables t inner join " +
                catalog + "sys.partitions p on t.object_id = p.object_id and p.index_id IN (0, 1) " +
                "where t.name = ? and schema_name(t.schema_id) = ?",
                table.getName(), table.getSchema());
    }

}
