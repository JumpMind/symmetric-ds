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
package org.jumpmind.db.platform.bigquery;

import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

import com.google.cloud.bigquery.BigQuery;

public class BigQueryPlatform extends AbstractDatabasePlatform {
    ISqlTemplate sqlTemplate;
    BigQuery bigquery;

    public BigQueryPlatform(SqlTemplateSettings settings, BigQuery bigquery) {
        super(settings);
        this.bigquery = bigquery;
        sqlTemplate = new BigQuerySqlTemplate(bigquery);
        this.ddlBuilder = new BigQueryDdlBuilder(bigquery);
        this.ddlReader = new BigQueryDdlReader(bigquery);
    }

    @Override
    public String getName() {
        return "bigquery";
    }

    @Override
    public String getDefaultSchema() {
        return null;
    }

    @Override
    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public <T> T getDataSource() {
        return null;
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

    @Override
    public ISqlTemplate getSqlTemplateDirty() {
        return null;
    }

    @Override
    public IDdlBuilder getDdlBuilder() {
        return this.ddlBuilder;
    }

    @Override
    public IDdlReader getDdlReader() {
        return new BigQueryDdlReader(this.bigquery);
    }

    public BigQuery getBigQuery() {
        return bigquery;
    }

    @Override
    public boolean supportsLimitOffset() {
        return true;
    }

    @Override
    public String massageForLimitOffset(String sql, int limit, int offset) {
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql + " limit " + limit + " offset " + offset + ";";
    }
}
