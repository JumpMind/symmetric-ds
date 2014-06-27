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
package org.jumpmind.symmetric.model;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.NestedDataWriter;

public class ProcessInfoDataWriter extends NestedDataWriter {

    private ProcessInfo processInfo;

    public ProcessInfoDataWriter(IDataWriter targetWriter, ProcessInfo processInfo) {
        super(targetWriter);
        this.processInfo = processInfo;
    }

    public void open(DataContext context) {
        super.open(context);
        processInfo.setDataCount(0);
        processInfo.setBatchCount(0);
    }

    public void start(Batch batch) {
        if (batch != null) {
            processInfo.setCurrentBatchId(batch.getBatchId());
            processInfo.setCurrentChannelId(batch.getChannelId());
            processInfo.incrementBatchCount();
        }
        super.start(batch);
    }

    public boolean start(Table table) {
        if (table != null) {
            processInfo.setCurrentTableName(table.getFullyQualifiedTableName());
        }
        return super.start(table);
    }

    public void write(CsvData data) {
        if (data != null) {
            processInfo.incrementDataCount();
        }
        super.write(data);        
    }

}
