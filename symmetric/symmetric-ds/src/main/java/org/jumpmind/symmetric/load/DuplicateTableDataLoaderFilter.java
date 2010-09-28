/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * 
 */
public class DuplicateTableDataLoaderFilter implements IDataLoaderFilter {
    private static final ILog log = LogFactory.getLog(DuplicateTableDataLoaderFilter.class);

    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private TableTemplate tableTemplate;

    private String duplicateSchema;

    private String duplicateCatalog;

    private String duplicateTableName;

    private String originalTableName;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        if (context.getTableName().equals(originalTableName)) {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.insert(context, keyValues);
        }
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        if (context.getTableName().equals(originalTableName)) {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.insert(context, columnValues);
        }
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        if (context.getTableName().equals(originalTableName)) {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.update(context, columnValues, keyValues);
        }
        return true;
    }

    private TableTemplate getTableTemplate(IDataLoaderContext context) {
        if (tableTemplate == null) {
            tableTemplate = new TableTemplate(jdbcTemplate, dbDialect, duplicateTableName, null, false,
                    duplicateSchema, duplicateCatalog);
            tableTemplate.setColumnNames(context.getColumnNames());
            tableTemplate.setKeyNames(context.getKeyNames());
        }
        return tableTemplate;
    }

    public boolean isAutoRegister() {
        log.info("TableDuplicating", originalTableName, (duplicateCatalog != null ? duplicateCatalog + "." : "")
                + (duplicateSchema != null ? duplicateSchema + "." : "") + duplicateTableName);
        return true;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setOriginalTableName(String tableName) {
        this.originalTableName = tableName;
    }

    public void setDuplicateTableName(String duplicateTableName) {
        this.duplicateTableName = duplicateTableName;
    }

    public void setDuplicateSchema(String schema) {
        this.duplicateSchema = schema;
    }

    public void setDuplicateCatalog(String catalog) {
        this.duplicateCatalog = catalog;
    }

}