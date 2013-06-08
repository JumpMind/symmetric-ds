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
package org.jumpmind.symmetric.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSyncZipDataWriter implements IDataWriter {

    static final Logger log = LoggerFactory.getLogger(FileSyncZipDataWriter.class);

    protected long byteCount;
    protected long maxBytesToSync;
    protected IFileSyncService fileSyncService;
    protected IStagedResource stagedResource;
    protected ZipOutputStream zos;
    protected Table snapshotTable;
    protected Batch batch;
    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();
    protected List<FileSnapshot> snapshotEvents;
    protected DataContext context;
    protected INodeService nodeService;

    public FileSyncZipDataWriter(long maxBytesToSync, IFileSyncService fileSyncService,
            INodeService nodeService, IStagedResource stagedResource) {
        this.maxBytesToSync = maxBytesToSync;
        this.fileSyncService = fileSyncService;
        this.stagedResource = stagedResource;
        this.nodeService = nodeService;
    }

    public void open(DataContext context) {
        this.context = context;
    }

    public void close() {
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    public void start(Batch batch) {
        this.batch = batch;
        this.statistics.put(batch, new Statistics());
        this.snapshotEvents = new ArrayList<FileSnapshot>();
    }

    public boolean start(Table table) {
        this.snapshotTable = table;
        return true;
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
            snapshot.setLastUpdateBy(columnData.get("LAST_UPDATE_BY"));
            snapshot.setFileName(columnData.get("FILE_NAME"));
            snapshot.setFilePath(columnData.get("FILE_PATH"));
            snapshot.setLastEventType(LastEventType.fromCode(columnData.get("LAST_EVENT_TYPE")));
            snapshotEvents.add(snapshot);
        } else if (eventType == DataEventType.RELOAD) {
            String targetNodeId = context.getBatch().getTargetNodeId();
            Node targetNode = nodeService.findNode(targetNodeId);
            List<FileTriggerRouter> fileTriggerRouters = fileSyncService
                    .getFileTriggerRoutersForCurrentNode();
            for (FileTriggerRouter fileTriggerRouter : fileTriggerRouters) {
                if (fileTriggerRouter.isEnabled()
                        && fileTriggerRouter.isInitialLoadEnabled()
                        && fileTriggerRouter.getRouter().getNodeGroupLink().getTargetNodeGroupId()
                                .equals(targetNode.getNodeGroupId())) {
                    DirectorySnapshot directorySnapshot = fileSyncService
                            .getDirectorySnapshot(fileTriggerRouter);
                    snapshotEvents.addAll(directorySnapshot);
                }
            }
        }
    }

    public void end(Table table) {
    }

    public void end(Batch batch, boolean inError) {

        try {
            if (!inError) {
                if (zos == null) {
                    zos = new ZipOutputStream(stagedResource.getOutputStream());
                }

                Set<String> commands = new HashSet<String>();
                StringBuilder script = new StringBuilder("fileList = new HashMap();\n");
                for (FileSnapshot snapshot : snapshotEvents) {
                    FileTriggerRouter triggerRouter = fileSyncService.getFileTriggerRouter(
                            snapshot.getTriggerId(), snapshot.getRouterId());
                    if (triggerRouter != null) {
                        StringBuilder command = new StringBuilder("\n");
                        LastEventType eventType = snapshot.getLastEventType();

                        FileTrigger fileTrigger = triggerRouter.getFileTrigger();

                        String targetBaseDir = triggerRouter.getTargetBaseDir();
                        if (StringUtils.isBlank(targetBaseDir)) {
                            targetBaseDir = fileTrigger.getBaseDir();
                        }
                        targetBaseDir = StringEscapeUtils.escapeJava(targetBaseDir);

                        command.append("targetBaseDir = \"").append(targetBaseDir).append("\";\n");
                        command.append("processFile = true;\n");
                        command.append("sourceFileName = \"").append(snapshot.getFileName())
                                .append("\";\n");
                        command.append("sourceFilePath = \"");
                        command.append(StringEscapeUtils.escapeJava(snapshot.getFilePath()))
                                .append("\";\n");

                        StringBuilder entryName = new StringBuilder(Long.toString(batch
                                .getBatchId()));
                        entryName.append("/");
                        if (!snapshot.getFilePath().equals(".")) {
                            entryName.append(snapshot.getFilePath()).append("/");
                        }
                        entryName.append(snapshot.getFileName());

                        File file = fileTrigger.createSourceFile(snapshot);
                        if (file.isDirectory()) {
                            entryName.append("/");
                        }

                        if (StringUtils.isNotBlank(fileTrigger.getBeforeCopyScript())) {
                            command.append(fileTrigger.getBeforeCopyScript()).append("\n");
                        }

                        command.append("if (processFile) {\n");

                        switch (eventType) {
                            case CREATE:
                            case MODIFY:
                            case SEED:
                                if (file.exists()) {
                                    command.append("  mv (batchDir + \"/\"");
                                    if (!snapshot.getFilePath().equals(".")) {
                                        command.append(" + sourceFilePath + \"/\"");
                                    }
                                    command.append(" + sourceFileName");
                                    command.append(", ");
                                    StringBuilder targetFile = new StringBuilder(
                                            "targetBaseDir + \"/");
                                    if (!snapshot.getFilePath().equals(".")) {
                                        targetFile.append(StringEscapeUtils.escapeJava(snapshot
                                                .getFilePath()));
                                        targetFile.append("/");
                                    }
                                    targetFile.append(snapshot.getFileName());
                                    targetFile.append("\"");
                                    command.append(targetFile);
                                    command.append(");\n");
                                    command.append("  fileList.put(").append(targetFile)
                                            .append(",\"");
                                    command.append(eventType.getCode());
                                    command.append("\");\n");
                                }
                                break;
                            case DELETE:
                                command.append("  org.apache.commons.io.FileUtils.deleteQuietly(new java.io.File(");
                                StringBuilder targetFile = new StringBuilder("targetBaseDir + \"/");
                                if (!snapshot.getFilePath().equals(".")) {
                                    targetFile.append(StringEscapeUtils.escapeJava(snapshot
                                            .getFilePath()));
                                    targetFile.append("/");
                                }
                                targetFile.append(snapshot.getFileName());
                                targetFile.append("\"");
                                command.append(targetFile);
                                command.append("));\n");
                                command.append("  fileList.put(").append(targetFile).append(",\"");
                                command.append(eventType.getCode());
                                command.append("\");\n");
                                break;
                            default:
                                break;
                        }

                        if (StringUtils.isNotBlank(fileTrigger.getAfterCopyScript())) {
                            command.append(fileTrigger.getAfterCopyScript()).append("\n");
                        }

                        if (!commands.contains(command.toString())) {
                            if (file.exists()) {
                                byteCount += file.length();
                                ZipEntry entry = new ZipEntry(entryName.toString());
                                entry.setSize(file.length());
                                entry.setTime(file.lastModified());
                                zos.putNextEntry(entry);
                                if (file.isFile()) {
                                    FileInputStream fis = new FileInputStream(file);
                                    try {
                                        IOUtils.copy(fis, zos);
                                    } finally {
                                        IOUtils.closeQuietly(fis);
                                    }
                                }
                                zos.closeEntry();
                            } else if (eventType != LastEventType.DELETE) {
                                log.warn(
                                        "Could not find the {} file to package for synchronization.  Skipping it.",
                                        file.getAbsolutePath());
                            }

                            command.append("}\n\n");
                            script.append(command.toString());
                            commands.add(command.toString());

                        }

                    } else {
                        log.error(
                                "Could not locate the file trigger ({}) router ({}) to process a snapshot event.  The event will be ignored",
                                snapshot.getTriggerId(), snapshot.getRouterId());
                    }
                }

                script.append("return fileList;\n");
                ZipEntry entry = new ZipEntry(batch.getBatchId() + "/sync.bsh");
                zos.putNextEntry(entry);
                IOUtils.write(script.toString(), zos);
                zos.closeEntry();

            }
        } catch (IOException e) {
            throw new IoException(e);
        }

    }

    public void finish() {
        try {
            if (zos != null) {
                zos.finish();
                IOUtils.closeQuietly(zos);
            }
        } catch (IOException e) {
            throw new IoException(e);
        } finally {
            stagedResource.close();
            stagedResource.setState(IStagedResource.State.READY);
        }
    }

    public boolean readyToSend() {
        return byteCount > maxBytesToSync;
    }

}
