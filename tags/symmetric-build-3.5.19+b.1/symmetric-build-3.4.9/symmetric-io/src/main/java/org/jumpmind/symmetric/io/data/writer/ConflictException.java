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
 * under the License. 
 */
package org.jumpmind.symmetric.io.data.writer;

import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;

public class ConflictException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected CsvData data;

    protected Table table;

    protected boolean fallbackOperationFailed = false;
    
    protected Conflict conflict;

    public ConflictException(CsvData data, Table table, boolean fallbackOperationFailed, Conflict conflict) {
        super(message(data, table, fallbackOperationFailed));
        this.data = data;
        this.table = table;
        this.fallbackOperationFailed = fallbackOperationFailed;
        this.conflict = conflict;
    }

    protected static String message(CsvData data, Table table, boolean fallbackOperationFailed) {
        Map<String, String> pks = data.toColumnNameValuePairs(table.getPrimaryKeyColumnNames(),
                CsvData.PK_DATA);
        if (pks == null || pks.size() == 0) {
            pks = data.toColumnNameValuePairs(table.getPrimaryKeyColumnNames(), CsvData.OLD_DATA);
        }

        if (pks == null || pks.size() == 0) {
            pks = data.toColumnNameValuePairs(table.getPrimaryKeyColumnNames(), CsvData.ROW_DATA);
        }

        return String.format(
                "Detected conflict while executing %s on %s.  The primary key data was: %s. %s",
                data.getDataEventType().toString(), table.getFullyQualifiedTableName(), pks,
                fallbackOperationFailed ? "Failed to fallback." : "");
    }

    public CsvData getData() {
        return data;
    }

    public Table getTable() {
        return table;
    }
    
    public Conflict getConflict() {
        return conflict;
    }

    public boolean isFallbackOperationFailed() {
        return fallbackOperationFailed;
    }

}
