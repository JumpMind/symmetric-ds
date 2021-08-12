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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.db.util.TableRow;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;

public class BigQueryDdlReader implements IDdlReader {
    BigQuery bigQuery;

    public BigQueryDdlReader(BigQuery bq) {
        this.bigQuery = bq;
    }

    @Override
    public Database readTables(String catalog, String schema, String[] tableTypes) {
        return null;
    }

    @Override
    public Table readTable(String catalog, String schema, String tableName) {
        com.google.cloud.bigquery.Table bqTable = this.bigQuery.getTable(TableId.of(schema, tableName));
        Table table = null;
        if (bqTable != null && bqTable.getDefinition() instanceof StandardTableDefinition) {
            StandardTableDefinition defn = (StandardTableDefinition) bqTable.getDefinition();
            table = new Table(catalog, schema, tableName);
            for (com.google.cloud.bigquery.Field bqField : defn.getSchema().getFields()) {
                Column column = new Column(bqField.getName(), false, getTypeCode(bqField.getType()), 0, 0);
                table.addColumn(column);
            }
        }
        return table;
    }

    protected int getTypeCode(LegacySQLTypeName legacyType) {
        int typeCode = Types.OTHER;
        if (legacyType.equals(LegacySQLTypeName.INTEGER)) {
            typeCode = Types.INTEGER;
        } else if (legacyType.equals(LegacySQLTypeName.BOOLEAN)) {
            typeCode = Types.BOOLEAN;
        } else if (legacyType.equals(LegacySQLTypeName.BYTES)) {
            typeCode = Types.BINARY;
        } else if (legacyType.equals(LegacySQLTypeName.DATE)) {
            typeCode = Types.DATE;
        } else if (legacyType.equals(LegacySQLTypeName.DATETIME)) {
            typeCode = Types.TIMESTAMP;
        } else if (legacyType.equals(LegacySQLTypeName.FLOAT)) {
            typeCode = Types.FLOAT;
        } else if (legacyType.equals(LegacySQLTypeName.NUMERIC)) {
            typeCode = Types.NUMERIC;
        } else if (legacyType.equals(LegacySQLTypeName.STRING)) {
            typeCode = Types.VARCHAR;
        } else if (legacyType.equals(LegacySQLTypeName.TIMESTAMP)) {
            typeCode = Types.TIMESTAMP;
        }
        return typeCode;
    }

    @Override
    public List<String> getTableTypes() {
        return null;
    }

    @Override
    public List<String> getCatalogNames() {
        return null;
    }

    @Override
    public List<String> getSchemaNames(String catalog) {
        Page<Dataset> datasets = this.bigQuery.listDatasets();
        List<String> schemas = new ArrayList<String>();
        while (datasets.hasNextPage()) {
            for (Dataset ds : datasets.getNextPage().getValues()) {
                schemas.add(ds.toString());
            }
        }
        return schemas;
    }

    @Override
    public List<String> getTableNames(String catalog, String schema, String[] tableTypes) {
        return null;
    }

    @Override
    public List<String> getColumnNames(String catalog, String schema, String tableName) {
        return null;
    }

    @Override
    public List<Trigger> getTriggers(String catalog, String schema, String tableName) {
        return null;
    }

    @Override
    public Trigger getTriggerFor(Table table, String name) {
        return null;
    }

    @Override
    public Collection<ForeignKey> getExportedKeys(Table table) {
        return null;
    }

    @Override
    public List<TableRow> getExportedForeignTableRows(ISqlTransaction transaction, List<TableRow> tableRows, Set<TableRow> visited, BinaryEncoding encoding) {
        return null;
    }

    @Override
    public List<TableRow> getImportedForeignTableRows(List<TableRow> tableRows, Set<TableRow> visited, BinaryEncoding encoding) {
        return null;
    }
}
