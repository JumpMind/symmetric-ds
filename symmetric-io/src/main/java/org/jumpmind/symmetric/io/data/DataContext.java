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
package org.jumpmind.symmetric.io.data;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.NestedDataWriter;
import org.jumpmind.util.Context;

public class DataContext extends Context {

    protected IDataWriter writer;

    protected IDataReader reader;

    protected Batch batch;

    protected Table table;

    protected CsvData data;
    
    protected Throwable lastError;
    
    protected Map<String, Table> parsedTables = new HashMap<String, Table>();
    
    protected Table lastParsedTable = null;

    public DataContext(Batch batch) {
        this.batch = batch;
    }

    public DataContext() {
    }

    public DataContext(IDataReader reader) {
        this.reader = reader;
    }

    public IDataReader getReader() {
        return reader;
    }

    public IDataWriter getWriter() {
        return writer;
    }
    
    public void setReader(IDataReader reader) {
        this.reader = reader;
    }

    protected void setWriter(IDataWriter writer) {
        this.writer = writer;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Batch getBatch() {
        return batch;
    }

    public void setData(CsvData data) {
        this.data = data;
    }

    public CsvData getData() {
        return data;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }
    
    public void setLastError(Throwable lastError) {
        this.lastError = lastError;
    }
    
    public Throwable getLastError() {
        return lastError;
    }
    
    public Map<String, Table> getParsedTables() {
        return parsedTables;
    }
    
    public Table getLastParsedTable() {
        return lastParsedTable;
    }
    
    public void setLastParsedTable(Table lastParsedTable) {
        this.lastParsedTable = lastParsedTable;
    }

    public ISqlTransaction findTransaction() {
        ISqlTransaction transaction = null;
        if (writer instanceof NestedDataWriter) {
            DatabaseWriter dbWriter = ((NestedDataWriter)writer).getNestedWriterOfType(DatabaseWriter.class);
            if (dbWriter != null) {
                transaction = dbWriter.getTransaction();
            }
        } else if (writer instanceof DatabaseWriter) {
            transaction = ((DatabaseWriter) writer).getTransaction();
        }
        return transaction;
    }

}
