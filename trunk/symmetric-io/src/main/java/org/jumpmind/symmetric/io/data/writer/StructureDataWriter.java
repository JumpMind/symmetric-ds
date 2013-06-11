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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

public class StructureDataWriter implements IDataWriter {

    protected IDatabasePlatform platform;

    protected Table currentTable;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    public enum PayloadType {
        CSV, SQL
    };

    // map is comprised of a batch and a list of payload data
    // that goes with the batch
    private Map<Long, List<String>> payloadMap = new HashMap<Long, List<String>>();

    private PayloadType payloadType = PayloadType.SQL;

    private long currentBatch;

    public StructureDataWriter(IDatabasePlatform platform, PayloadType payloatType) {
        this.platform = platform;
        this.payloadType = payloatType;
    }

    public void open(DataContext context) {
    }

    public void close() {
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    public void start(Batch batch) {
        List<String> payloadData = new ArrayList<String>();
        this.currentBatch = batch.getBatchId();
        this.statistics.put(batch, new Statistics());
        this.payloadMap.put(currentBatch, payloadData);
    }

    public boolean start(Table table) {
        this.currentTable = table;
        return true;
    }

    public void write(CsvData data) {
        DmlStatement dml = null;
        switch (data.getDataEventType()) {
            case UPDATE:
                dml = platform.createDmlStatement(DmlType.UPDATE, currentTable);
                break;
            case INSERT:
                dml = platform.createDmlStatement(DmlType.INSERT, currentTable);
                break;
            case DELETE:
                dml = platform.createDmlStatement(DmlType.DELETE, currentTable);
                break;
            case SQL:
                // TODO: figure out what to do with these
                break;
            case CREATE:
                // TODO: figure out what to do with these
                break;
            default:
                break;
        }
        // TODO: change the ? to the actual data
        // platform.replaceSql(dml.getSql(), BinaryEncoding.NONE, currentTable,
        // data.getParsedData(CsvData.ROW_DATA),
        // true);

        this.payloadMap.get(this.currentBatch).add(dml.getSql());
    }

    public void end(Table table) {
    }

    public void end(Batch batch, boolean inError) {
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(PayloadType payloadType) {
        this.payloadType = payloadType;
    }

    public Map<Long, List<String>> getPayloadMap() {
        return payloadMap;
    }

    public void setPayloadMap(Map<Long, List<String>> payloadMap) {
        this.payloadMap = payloadMap;
    }

}
