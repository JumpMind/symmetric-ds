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
package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;

public class JdbcDatabaseWriter extends DynamicDefaultDatabaseWriter {

    String tablePrefix;

    public JdbcDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
            String tablePrefix) {
        super(symmetricPlatform, targetPlatform, tablePrefix);
    }

    public void setTablePrefix(String prefix) {
        this.tablePrefix = prefix;
    }

    @Override
    public void start(Batch batch) {
        super.start(batch);
    }

    @Override
    public boolean start(Table table) {
        if (table == null) {
            throw new NullPointerException("Cannot load a null table");
        }
        this.lastData = null;
        this.sourceTable = table;
        try {
            this.targetTable = lookupTableAtTarget(this.sourceTable);
            if (targetTable == null) {
                this.targetTable = sourceTable;
            }
        } catch (Exception e) {
            this.targetTable = this.sourceTable;
        }
        this.sourceTable.copyColumnTypesFrom(this.targetTable);
        if (this.targetTable == null && hasFilterThatHandlesMissingTable(table)) {
            this.targetTable = table;
        }

        /*
         * The first data that requires a target table should fail because the table
         * will not be found
         */
        return true;
    }

    @Override
    protected LoadStatus insert(CsvData data) {
        return super.insert(data);
    }

    @Override
    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        if (targetTable.getName().startsWith(tablePrefix)) {
            return LoadStatus.SUCCESS;
        }
        return super.update(data, applyChangesOnly, useConflictDetection);
    }

    @Override
    protected boolean sql(CsvData data) {
        if (targetTable.getName().startsWith(tablePrefix)) {
            return true;
        }
        return super.sql(data);
    }

    @Override
    public void write(CsvData data) {
        super.write(data);
    }
}
