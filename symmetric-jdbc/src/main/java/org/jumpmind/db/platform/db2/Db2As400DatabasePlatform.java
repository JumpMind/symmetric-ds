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

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class Db2As400DatabasePlatform extends Db2DatabasePlatform {

    public Db2As400DatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        supportsTruncate = false;
    }

    @Override
    protected Db2DdlReader createDdlReader() {
        return new Db2As400DdlReader(this);
    }

    @Override
    protected Db2DdlBuilder createDdlBuilder() {
        return new Db2As400DdlBuilder();
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            try {
                defaultSchema = (String) getSqlTemplate().queryForObject("select CURRENT SCHEMA from sysibm.sysdummy1", String.class);
            } catch (Exception e) {
                try {
                    defaultSchema = (String) getSqlTemplate().queryForObject("select CURRENT SCHEMA from QSYS2.QSQPTABL", String.class);
                } catch(Exception ex) {
                    defaultSchema = "";
                }
            }
        }
        return defaultSchema;
    }
    
    @Override
    public String getDefaultCatalog() {
        // This must return null for AS400, an empty string will return no match on readTable meta data.
        return null;
    }

    @Override
    protected PermissionResult getCreateSymTablePermission(Database database) {
        Table table = getPermissionTableDefinition();

        PermissionResult result = new PermissionResult(PermissionType.CREATE_TABLE, "creating table " + table.getName() + "...");
        getDropSymTablePermission();

        try {
            database.addTable(table);
            createDatabase(database, false, false);

            ISqlTransaction tran = null;
            try {
                tran = sqlTemplate.startSqlTransaction();
                tran.prepareAndExecute("insert into " + table.getName() + " values (?, ?)", 1, 0);
                result.setStatus(Status.PASS);
            } catch (SqlException e) {
                result.setException(e);
                result.setSolution("Enable automatic journaling on library");
            } finally {
                if (tran != null) {
                    tran.rollback();
                    tran.close();
                }
            }
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE permission");
        }

        return result;
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.DB2AS400;
    }

}
