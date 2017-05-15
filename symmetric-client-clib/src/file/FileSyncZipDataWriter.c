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
#include "file/FileSyncZipDataWriter.h"
#include "service/FileSyncService.h"


void SymFileSyncZipDataWriter_open(SymFileSyncZipDataWriter *this) {
    char * ZIP_NAME = "tmp/staging/filesync_outgoing/filesync.zip";

    SymFileUtils_mkdir("tmp/staging/filesync_outgoing/");

    int rc;
    this->zip = zip_open(ZIP_NAME, ZIP_CREATE|ZIP_TRUNCATE, &rc);
    if (!this->zip) {
        zip_error_t error;
        zip_error_init_with_code(&error, rc);
        SymLog_error("Could not start zip %s %s", ZIP_NAME, zip_error_strerror(&error));
        zip_error_fini(&error);
    }
}


static void SymFileSyncZipDataWriter_startBatch(SymFileSyncZipDataWriter *this, SymBatch *batch) {
    if (this->isFirstBatch) {
        this->isFirstBatch = 0;
    }

    this->batch = batch;
    this->snapshotEvents = SymList_new(NULL);
}

static void SymFileSyncZipDataWriter_endBatch(SymFileSyncZipDataWriter *this, SymBatch *batch) {
    SymMap * /*<char*, char*>*/ entries = SymMap_new(NULL, 4);
    SymStringBuilder *script = SymStringBuilder_newWithString("fileList = new HashMap();\n");
    int i;
    for (i = 0; i < this->snapshotEvents->size; ++i) {
        SymFileSnapshot *snapshot = this->snapshotEvents->get(this->snapshotEvents, i);
        SymFileTriggerRouter *triggerRouter =
                this->fileSyncService->getFileTriggerRouter(this->fileSyncService, snapshot->triggerId, snapshot->routerId);
        if (triggerRouter != NULL) {
            SymStringBuilder *command = SymStringBuilder_new("\n");
            char eventType = snapshot->lastEventType[0];

            SymFileTrigger *fileTrigger = triggerRouter->fileTrigger;
            char* targetBaseDir = triggerRouter->targetBaseDir;
            if (SymStringUtils_isBlank(targetBaseDir)) {
                targetBaseDir = fileTrigger->baseDir;
            }

            command->append(command, "targetBaseDir = \"")->append(command, targetBaseDir)->append(command, "\";\n");

            command->append(command, "if (targetBaseDir.startsWith(\"${androidBaseDir}\")) {                      \n");
            command->append(command, "    targetBaseDir = targetBaseDir.replace(\"${androidBaseDir}\", \"\");     \n");
            command->append(command, "    targetBaseDir = androidBaseDir + targetBaseDir;                         \n");
            command->append(command, "} else if (targetBaseDir.startsWith(\"${androidAppFilesDir}\")) {           \n");
            command->append(command, "    targetBaseDir = targetBaseDir.replace(\"${androidAppFilesDir}\", \"\"); \n");
            command->append(command, "    targetBaseDir = androidAppFilesDir + targetBaseDir;                     \n");
            command->append(command, "}                                                                           \n");

            command->append(command, "processFile = true;\n");
            command->append(command, "sourceFileName = \"")->append(command, snapshot->fileName)->append(command, "\";\n");
            command->append(command, "targetRelativeDir = \"");

            if (!SymStringUtils_equals(snapshot->relativeDir, ".")) {
                command->append(command, snapshot->relativeDir);
                command->append(command, "\";\n");
            } else {
                command->append(command, "\";\n");
            }

            command->append(command, "targetFileName = sourceFileName;\n");
            command->append(command, "sourceFilePath = \"");
            command->append(command, snapshot->relativeDir)->append(command, "\";\n");


            SymStringBuilder *entryName = SymStringBuilder_newWithString(SymStringUtils_format("%ld", batch->batchId));
            entryName->append(entryName, "/");
            if (!SymStringUtils_equals(snapshot->relativeDir, ".")) {
                entryName->append(entryName, snapshot->relativeDir)->append(entryName, "/");
            }
            entryName->append(entryName, snapshot->fileName);

            char* path = triggerRouter->fileTrigger->getPath(triggerRouter->fileTrigger, snapshot);
            if (SymFileUtils_isDir(path)) {
                entryName->append(entryName, "/");
            }

            if (SymStringUtils_isNotBlank(fileTrigger->beforeCopyScript)) {
                command->append(command, fileTrigger->beforeCopyScript);
                command->append(command, "\n");
            }
            command->append(command, "if (processFile) {\n");
            char* targetFile = "targetBaseDir + \"/\" + targetRelativeDir + \"/\" + targetFileName";

            switch (eventType) {
            case 'C':
            case 'M':
                if (SymFileUtils_exists(path)) {
                    command->append(command, "  File targetBaseDirFile = new File(targetBaseDir);\n");
                    command->append(command, "  if (!targetBaseDirFile.exists()) {\n");
                    command->append(command, "    targetBaseDirFile.mkdirs();\n");
                    command->append(command, "  }\n");
                    command->append(command, "  java.io.File sourceFile = new java.io.File(batchDir + \"/\"");

                    if (!SymStringUtils_equals(snapshot->relativeDir, ".")) {
                        command->append(command, " + sourceFilePath + \"/\"");
                    }
                    command->append(command, " + sourceFileName");
                    command->append(command, ");\n");

                    command->append(command, "  java.io.File targetFile = new java.io.File(");
                    command->append(command, targetFile);
                    command->append(command, ");\n");

                    // no need to copy directory if it already exists
                    command->append(command, "  if (targetFile.exists() && targetFile.isDirectory()) {\n");
                    command->append(command, "      processFile = false;\n");
                    command->append(command, "  }\n");

                    //                        // conflict resolution
                    //                        FileConflictStrategy conflictStrategy = triggerRouter.getConflictStrategy();
                    //                        if (conflictStrategy == FileConflictStrategy.TARGET_WINS ||
                    //                                conflictStrategy == FileConflictStrategy.MANUAL) {
                    //                            command->append(command, "  if (targetFile.exists() && !targetFile.isDirectory()) {\n");
                    //                            command->append(command, "    long targetChecksum = org.apache.commons.io.FileUtils.checksumCRC32(targetFile);\n");
                    //                            command->append(command, "    if (targetChecksum != " + snapshot.getOldCrc32Checksum() + "L) {\n");
                    //                            if (conflictStrategy == FileConflictStrategy.MANUAL) {
                    //                                command->append(command, "      throw new org.jumpmind.symmetric.file.FileConflictException(targetFileName + \" was in conflict \");\n");
                    //                            } else {
                    //                                command->append(command, "      processFile = false;\n");
                    //                            }
                    //                            command->append(command, "    }\n");
                    //                            command->append(command, "  }\n");
                    //                        }

                    command->append(command, "  if (processFile) {\n");
                    command->append(command, "    if (sourceFile.isDirectory()) {\n");
                    command->append(command, "      org.apache.commons.io.FileUtils.copyDirectory(sourceFile, targetFile, true);\n");
                    command->append(command, "    } else {\n");
                    command->append(command, "      org.apache.commons.io.FileUtils.copyFile(sourceFile, targetFile, true);\n");
                    command->append(command, "    }\n");
                    command->append(command, "  }\n");
                    command->append(command, "  fileList.put(")->append(command, targetFile)->append(command, ",\"");
                    command->appendf(command, "%c", eventType);
                    command->append(command, "\");\n");
                }
                break;
            case 'D':
                command->append(command, "  org.apache.commons.io.FileUtils.deleteQuietly(new java.io.File(");
                command->append(command, targetFile);
                command->append(command, "));\n");
                command->append(command, "  fileList.put(")->append(command, targetFile)->append(command, ",\"");
                command->appendf(command, "%c", eventType);
                command->append(command, "\");\n");
                break;
            default:
                break;
            }

            if (SymStringUtils_isNotBlank(fileTrigger->afterCopyScript)) {
                command->append(command, fileTrigger->afterCopyScript)->append(command, "\n");
            }

            char* previousEventForEntryString = (char*)entries->get(entries, entryName->toString(entryName));

            unsigned short process = 1;
            if (previousEventForEntryString != NULL) {
                char previousEventForEntry = previousEventForEntryString[0];
                if ((previousEventForEntry == eventType)
                        || (previousEventForEntry == 'C' && eventType == 'M')) {
                    process = 0;
                }
            }

            if (process) {
                if (eventType != 'D') {
                    if (SymFileUtils_exists(path)) {
                        long fileSize = SymFileUtils_getFileSize(path);
                        this->byteCount += fileSize;

                        long START_POSITION = 0, END_POSITION = 0;
                        if (!this->zip) {
                            SymLog_error("zip is not initialized, seg fault imminent.");
                        }
                        if (SymFileUtils_isDir(path)) {
                            zip_add_dir(this->zip, entryName->toString(entryName));
                        } else {
                            zip_source_t *zipSource = zip_source_file(this->zip, path, START_POSITION, END_POSITION);
                            if (!zipSource) {
                                SymLog_error("error creating zip source from path '%s': %s\n", path, zip_strerror(this->zip));
                            }
                            long zipIndex = zip_file_add(this->zip, entryName->toString(entryName), zipSource, ZIP_FL_OVERWRITE);
                            if (zipIndex < 0) {
                                SymLog_error("error adding file '%s': %s\n", path, zip_strerror(this->zip));
                            }
                        }
                        entries->put(entries, entryName->toString(entryName), SymStringUtils_format("%c", eventType));
                    } else {
                        SymLog_warn("Could not find the %s file to package for synchronization.  Skipping it.", path);
                    }
                }
                command->append(command, "}\n\n");
                script->append(script, command->destroyAndReturn(command));
                entryName->destroy(entryName);
            }
        } else {
            SymLog_error("Could not locate the file trigger (%s) router (%s) to process a snapshot event.  The event will be ignored",
                    snapshot->triggerId, snapshot->routerId);
        }

    }
    script->append(script, "return fileList;\n");

    {
        char * bshFileName = SymStringUtils_format("%ld/sync.bsh", batch->batchId);
        zip_source_t *zipSourceBuffer = zip_source_buffer(this->zip, script->toString(script), script->size-1, 0);
        if (!zipSourceBuffer) {
            printf("error creating zip source buffer: %s\n", zip_strerror(this->zip));
        }
        long zipIndex = zip_file_add(this->zip, bshFileName, zipSourceBuffer, ZIP_FL_OVERWRITE);
        if (zipIndex < 0) {
            printf("error adding buffer: %s\n", zip_strerror(this->zip));
        }
        free(bshFileName);
    }
    {
        char * batchInfoFileName = SymStringUtils_format("%ld/batch-info.txt", batch->batchId);
        char * channelIdClone = SymStringUtils_format("%s", batch->channelId); // The batch will be freed before the zip gets written.
        zip_source_t *zipSourceBuffer = zip_source_buffer(this->zip, channelIdClone, strlen(channelIdClone), 0);
        if (!zipSourceBuffer) {
            printf("error creating zip source buffer: %s\n", zip_strerror(this->zip));
        }
        long zipIndex = zip_file_add(this->zip, batchInfoFileName, zipSourceBuffer, ZIP_FL_OVERWRITE);
        if (zipIndex < 0) {
            printf("error adding buffer: %s\n", zip_strerror(this->zip));
        }
        free(batchInfoFileName);
//        free(channelIdClone);
    }

//    script->destroy(script);
    entries->destroy(entries);
    this->batch = NULL;
}

static void SymFileSyncZipDataWriter_startTable(SymFileSyncZipDataWriter *this, SymTable *table) {
    this->snapshotTable = table;
}

static void SymFileSyncZipDataWriter_endTable(SymFileSyncZipDataWriter *this, SymTable *table) {
    // no-op.
}

unsigned short SymFileSyncZipDataWriter_write(SymFileSyncZipDataWriter *this, SymCsvData *data) {
    SymDataEventType eventType = data->dataEventType;

    if (eventType == SYM_DATA_EVENT_INSERT || eventType == SYM_DATA_EVENT_UPDATE) {
        //  SymLog_info("data = %s", data->rowData->toString(data->rowData));

        //     SymList * /*<SymColumn>*/ columns = this->snapshotTable->columns;
        SymStringArray *columnNames = this->snapshotTable->getColumnNames(this->snapshotTable);
        // SymLog_info("columnNames = %s", columnNames->toString(columnNames));
        SymMap * /*<String,String>*/ columnData = data->toColumnNameValuePairsRowData(data, columnNames);
        SymMap * /*<String,String>*/ oldColumnData = data->toColumnNameValuePairsOldData(data, columnNames);

        SymFileSnapshot *snapshot = SymFileSnapshot_new(NULL);
        snapshot->triggerId = columnData->get(columnData, "trigger_id");
        snapshot->routerId = columnData->get(columnData, "router_id");
        char* modifedTime = columnData->get(columnData, "file_modified_time");
        if (!SymStringUtils_isBlank(modifedTime)) {
            snapshot->fileModifiedTime = atol(modifedTime);
        }
        char *crc32Checksum = columnData->get(columnData, "crc32_checksum");
        if (!SymStringUtils_isBlank(crc32Checksum)) {
            snapshot->crc32Checksum = atol(crc32Checksum);
        }

        char *oldChecksum = oldColumnData->get(oldColumnData, "crc32_checksum");
        if (!SymStringUtils_isBlank(oldChecksum)) {
            snapshot->oldCrc32Checksum = atol(oldChecksum);
        }
        char *fileSize = columnData->get(columnData, "file_size");
        if (!SymStringUtils_isBlank(fileSize)) {
            snapshot->fileSize = atol(fileSize);
        }
        snapshot->lastUpdateBy = columnData->get(columnData, "last_update_by");
        snapshot->fileName = columnData->get(columnData, "file_name");
        snapshot->relativeDir = columnData->get(columnData, "relative_dir");
        snapshot->lastEventType = columnData->get(columnData, "last_event_type");
        this->snapshotEvents->add(this->snapshotEvents, snapshot);
    } else if (eventType == SYM_DATA_EVENT_RELOAD) {
        char *targetNodeId = this->context->batch->targetNodeId;
        SymNode *targetNode = this->nodeService->findNode(this->nodeService, targetNodeId);
        SymList * /*<SymFileTriggerRouter>*/ fileTriggerRouters =
                this->fileSyncService->getFileTriggerRoutersForCurrentNode(this->fileSyncService);
        int i;
        for (i = 0; i < fileTriggerRouters->size; ++i) {
            SymFileTriggerRouter *fileTriggerRouter = fileTriggerRouters->get(fileTriggerRouters, i);
            if (fileTriggerRouter->enabled
                    && SymStringUtils_equals(fileTriggerRouter->router->nodeGroupLink->targetNodeGroupId,
                            targetNode->nodeGroupId)) {
                SymDirectorySnapshot *directorySnapshot =
                        this->fileSyncService->getDirectorySnapshot(this->fileSyncService, fileTriggerRouter);
                this->snapshotEvents->addAll(this->snapshotEvents, directorySnapshot->fileSnapshots);
            }
        }
    }

    return 0;
}

SymList * SymFileSyncZipDataWriter_getBatchesProcessed(SymFileSyncZipDataWriter *this) {
    return this->super.batchesProcessed;
}

void SymFileSyncZipDataWriter_close(SymFileSyncZipDataWriter *this) {
    int rc = zip_close(this->zip);
    if (rc != 0) {
        SymLog_error("error closing zip file: %s\n", zip_strerror(this->zip));
    }
}

void SymFileSyncZipDataWriter_destroy(SymFileSyncZipDataWriter *this) {
    this->processedTables->destroy(this->processedTables);
    free(this);
}

SymFileSyncZipDataWriter * SymFileSyncZipDataWriter_new(SymFileSyncZipDataWriter *this, char *sourceNodeId) {
    if (this == NULL) {
        this = (SymFileSyncZipDataWriter *) calloc(1, sizeof(SymFileSyncZipDataWriter));
    }
    this->sourceNodeId = sourceNodeId;
    this->isFirstBatch = 1;
    this->processedTables = SymMap_new(NULL, 100);

    SymDataWriter *super = &this->super;

    super->batchesProcessed = SymList_new(NULL);
    super->open = (void *) &SymFileSyncZipDataWriter_open;
    super->startBatch = (void *) &SymFileSyncZipDataWriter_startBatch;
    super->startTable = (void *) &SymFileSyncZipDataWriter_startTable;
    super->write = (void *) &SymFileSyncZipDataWriter_write;
    super->endTable = (void *) &SymFileSyncZipDataWriter_endTable;
    super->endBatch = (void *) &SymFileSyncZipDataWriter_endBatch;
    super->close = (void *) &SymFileSyncZipDataWriter_close;
    super->destroy = (void *) &SymFileSyncZipDataWriter_destroy;
    return this;
}
