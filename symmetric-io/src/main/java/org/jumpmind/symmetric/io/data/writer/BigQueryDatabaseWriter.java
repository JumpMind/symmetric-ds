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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.bigquery.BigQueryPlatform;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;

public class BigQueryDatabaseWriter extends DynamicDefaultDatabaseWriter {
	private static final Logger log = LoggerFactory.getLogger(BigQueryDatabaseWriter.class);
    BigQuery bigquery;
    InsertAllRequest.Builder insertAllRequestBuilder;
    TableId currentTableId;
    
    int maxRowsToInsertPerRPC;
    int rowsAdded;
    int updateCount;
    int deleteCount;
   
    public BigQueryDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, String prefix,
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings, int maxRowsPerRpc) {
        super(symmetricPlatform, targetPlatform, prefix, conflictResolver, settings);
        this.maxRowsToInsertPerRPC = maxRowsPerRpc;
        bigquery = ((BigQueryPlatform) targetPlatform).getBigQuery();
    }


    @Override
    protected void prepare() {
        if (isSymmetricTable(this.targetTable != null ? this.targetTable.getName() : "")) {
            super.prepare();
        } else {
            
        }
    }
    
    @Override
    protected void prepare(String sql, CsvData data) {
        
    }
    
    /*
     * Checks if a send sql event type was for the sym_node table.  If it is the send sql shoudl run against Cassandra tables otherwise it is an internal Symmetric
     * send sql.
     */
    protected boolean isUserSendSql(String sql, CsvData data) {
        return data.getDataEventType() == DataEventType.SQL 
                && this.targetTable.getNameLowerCase().equals(this.getTablePrefix().toLowerCase() + "_node")
                && !sql.toLowerCase().contains("from " + this.getTablePrefix().toLowerCase() + "_node");
    }
    
    @Override
    public int prepareAndExecute(String sql, CsvData data) {
        return 1;
    }

    @Override
    protected int execute(CsvData data, String[] values) {
        if (isSymmetricTable(this.targetTable != null ? this.targetTable.getName() : "")) {
            return super.execute(data, values);
        } 
        
        if (this.currentDmlStatement.getDmlType() == DmlType.INSERT) {
        
            if (currentTableId == null) {
                currentTableId = TableId.of(this.targetTable.getSchema(), this.targetTable.getName());
            }
            
            if (insertAllRequestBuilder == null) {
                insertAllRequestBuilder = InsertAllRequest.newBuilder(currentTableId);
            }
            
            if (rowsAdded < maxRowsToInsertPerRPC) {
                Map<String, Object> map = IntStream.range(0, this.currentDmlStatement.getColumns().length).boxed()
                        .collect(Collectors.toMap(i -> (String) this.currentDmlStatement.getColumns()[i].getName(), i -> values[i]));
                
                insertAllRequestBuilder.addRow(RowToInsert.of(map));
                rowsAdded++;
            }
            
            if (rowsAdded == maxRowsToInsertPerRPC) {
                bigquery.insertAll(insertAllRequestBuilder.build());
                rowsAdded = 0;
                insertAllRequestBuilder = null;
            }
        } else if (this.currentDmlStatement.getDmlType() == DmlType.UPDATE) {
            updateCount++;
        } else if (this.currentDmlStatement.getDmlType() == DmlType.DELETE) {
            deleteCount++;
        }
        
        return 1;
    }

    @Override
    public void start(Batch batch) {
        super.start(batch);
        updateCount=0;
        deleteCount=0;
    }
    
    @Override
    public void end(Table table) {
        super.end(table);
        
        if (!isSymmetricTable(this.targetTable != null ? this.targetTable.getName() : "") && insertAllRequestBuilder != null) {
            bigquery.insertAll(insertAllRequestBuilder.build());
            rowsAdded = 0;
            insertAllRequestBuilder = null;
            currentTableId = null;
        }
    }
    
    @Override
    public void end(Batch batch, boolean inError) {
        super.end(batch, inError);
        
        if (updateCount > 0 || deleteCount > 0) {
            log.warn("Google BigQuery only supported for inserts, detected " + updateCount + 
                    " updates and " + deleteCount + " deletes, which will not be replicated.");
        }
    }
    
    
    @Override
    protected Table lookupTableAtTarget(Table sourceTable) {
        if (sourceTable != null && isSymmetricTable(sourceTable.getName())) {
            return super.lookupTableAtTarget(sourceTable);
        }

        return sourceTable;
    }
}
