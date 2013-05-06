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
package org.jumpmind.symmetric.file;

import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.util.Statistics;

public class FileSyncBatchDataWriter implements IDataWriter {

    protected long maxBytesToSync;
    protected IFileSyncService fileSyncService;
    
    public FileSyncBatchDataWriter(long maxBytesToSync, IFileSyncService fileSyncService) {
        this.maxBytesToSync = maxBytesToSync;
        this.fileSyncService = fileSyncService;
    }
    
    public void open(DataContext context) {
    }

    public void close() {
    }

    public Map<Batch, Statistics> getStatistics() {
        return null;
    }

    public void start(Batch batch) {
    }

    public boolean start(Table table) {
        return false;
    }

    public void write(CsvData data) {
    }

    public void end(Table table) {
    }

    public void end(Batch batch, boolean inError) {
    }
    
    public boolean readyToSend() {
        return true;
    }
    
    public String toManifest() {
        return null;
    }

}
