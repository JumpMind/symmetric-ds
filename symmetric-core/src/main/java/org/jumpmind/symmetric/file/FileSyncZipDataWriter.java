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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSyncZipDataWriter implements IDataWriter {

    static final Logger log = LoggerFactory.getLogger(FileSyncZipDataWriter.class);

    protected long maxBytesToSync;
    protected IFileSyncService fileSyncService;
    protected IStagedResource stagedResource;
    protected ZipOutputStream zos;
    protected Table snapshotTable;
    protected Batch batch;
    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();
    protected List<FileSnapshot> snapshotEvents;

    public FileSyncZipDataWriter(long maxBytesToSync, IFileSyncService fileSyncService,
            IStagedResource stagedResource) {
        this.maxBytesToSync = maxBytesToSync;
        this.fileSyncService = fileSyncService;
        this.stagedResource = stagedResource;
    }

    public void open(DataContext context) {
    }

    public void close() {
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    public void start(Batch batch) {
        this.batch = batch;
        this.snapshotEvents = new ArrayList<FileSnapshot>();
    }

    public boolean start(Table table) {
        this.snapshotTable = table;
        return false;
    }

    public void write(CsvData data) {
        DataEventType eventType = data.getDataEventType();
        if (eventType == DataEventType.INSERT || eventType == DataEventType.UPDATE) {
            Map<String, String> columnData = data.toColumnNameValuePairs(
                    snapshotTable.getColumnNames(), CsvData.ROW_DATA);
            FileSnapshot snapshot = new FileSnapshot();
            snapshot.setTriggerId(columnData.get("TRIGGER_ID"));
            snapshot.setRouterId(columnData.get("ROUTER_ID"));
            snapshot.setFileModifiedTime(FormatUtils.parseDate(
                    columnData.get("FILE_MODIFIED_TIME"), FormatUtils.TIMESTAMP_PATTERNS));
            snapshot.setCrc32Checksum(Long.parseLong(columnData.get("CRC32_CHECKSUM")));
            snapshot.setFileSize(Long.parseLong(columnData.get("FILE_SIZE")));
            snapshot.setFileName(columnData.get("FILE_NAME"));
            snapshot.setFilePath(columnData.get("FILE_PATH"));
            snapshot.setLastEventType(LastEventType.fromCode(columnData.get("LAST_EVENT_TYPE")));
            snapshotEvents.add(snapshot);
        }
    }

    public void end(Table table) {
    }

    public void end(Batch batch, boolean inError) {

        if (!inError) {
            if (zos == null) {
                zos = new ZipOutputStream(stagedResource.getOutputStream());
            }

            StringBuilder script = new StringBuilder();
            for (FileSnapshot snapshot : snapshotEvents) {
                FileTriggerRouter triggerRouter = fileSyncService.getFileTriggerRouter(
                        snapshot.getTriggerId(), snapshot.getRouterId());
                if (triggerRouter != null) {
                    FileTrigger fileTrigger = triggerRouter.getFileTrigger();
                    String targetBaseDir = triggerRouter.getTargetBaseDir();
                    if (StringUtils.isBlank(targetBaseDir)) {
                        targetBaseDir = fileTrigger.getBaseDir();
                    }
                    
                    script.append("cd ").append(targetBaseDir).append("\n");
                    
                    if (StringUtils.isNotBlank(fileTrigger.getBeforeCopyScript())) {
                        script.append(fileTrigger.getBeforeCopyScript()).append("\n");
                    }
                    
                    LastEventType eventType = snapshot.getLastEventType();
                    
                    switch (eventType) {
                        case CREATE:
                        case MODIFY:
                        case SEED:
                            script.append("cd ").append(targetBaseDir).append("\n");
                            
                            break;
                        case DELETE:
                            break;
                        default:
                            break;
                    }
                } else {
                    log.error(
                            "Could not locate the file trigger ({}) router ({}) to process a snapshot event.  The event will be ignored",
                            snapshot.getTriggerId(), snapshot.getRouterId());
                }
            }
        }

    }

    public boolean readyToSend() {
        return true;
    }

}
