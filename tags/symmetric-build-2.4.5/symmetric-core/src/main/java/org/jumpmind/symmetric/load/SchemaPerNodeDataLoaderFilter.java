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

import org.jumpmind.symmetric.db.IDbDialect;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * An extension that prefixes the table name with a schema name that is equal to the incoming node_id.
 *
 * ,
 */
public class SchemaPerNodeDataLoaderFilter implements IDataLoaderFilter {
    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private String tablePrefix;

    private String schemaPrefix;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        filter(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        filter(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        filter(context);
        return true;
    }

    private void filter(IDataLoaderContext context) {
        if (!context.getTableName().startsWith(tablePrefix)
                && !context.getSourceNodeId().equals(context.getTableTemplate().getTable().getSchema())) {
            ((DataLoaderContext) context).setTableTemplate(getTableTemplate(context));
        }
    }

    private TableTemplate getTableTemplate(IDataLoaderContext context) {
        TableTemplate tableTemplate = new TableTemplate(jdbcTemplate, dbDialect, context.getTableName(), null, false,
                schemaPrefix == null ? context.getSourceNodeId() : schemaPrefix + context.getSourceNodeId(), null);
        tableTemplate.setColumnNames(context.getColumnNames());
        tableTemplate.setKeyNames(context.getKeyNames());
        tableTemplate.setOldData(context.getOldData());
        return tableTemplate;
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }

}