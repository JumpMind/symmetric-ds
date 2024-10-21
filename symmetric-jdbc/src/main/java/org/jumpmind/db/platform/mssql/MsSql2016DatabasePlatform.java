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

import javax.sql.DataSource;

import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class MsSql2016DatabasePlatform extends MsSql2008DatabasePlatform {
    public static final String JDBC_SUBPROTOCOL = "sqlserver";
    public static final int SP1_BUILD_NUMBER = 4001;

    public MsSql2016DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        if (settings.isAllowTriggerCreateOrReplace()) {
            boolean triggersCreateOrReplaceSupported = true;
            String versionString = sqlTemplateDirty.queryForString("select serverproperty('ProductVersion')", (Object[]) null);
            if (versionString != null) {
                String[] versionArray = versionString.split("[.]");
                if (versionArray.length == 4) {
                    int majorVersion = Integer.parseInt(versionArray[0]);
                    if (majorVersion == 13) {
                        int buildNumber = Integer.parseInt(versionArray[2]);
                        triggersCreateOrReplaceSupported = (buildNumber >= SP1_BUILD_NUMBER);
                    }
                }
            }
            getDatabaseInfo().setTriggersCreateOrReplaceSupported(triggersCreateOrReplaceSupported);
        }
    }

    @Override
    protected MsSqlDdlReader createDdlReader() {
        return new MsSql2016DdlReader(this);
    }

    @Override
    protected IDdlBuilder createDdlBuilder() {
        return new MsSql2016DdlBuilder();
    }

    @Override
    public String getName() {
        return DatabaseNamesConstants.MSSQL2016;
    }

    /**
     * Signal support for VARCHAR(MAX) and NVARCHAR(MAX) in triggers and without handling them like CLOB types (no special read/write functions needed)
     */
    @Override
    public boolean isClob(int type) {
        if (type == ColumnTypes.MSSQL_VARCHARMAX || type == ColumnTypes.MSSQL_NVARCHARMAX) {
            return false;
        }
        return super.isClob(type);
    }
}
