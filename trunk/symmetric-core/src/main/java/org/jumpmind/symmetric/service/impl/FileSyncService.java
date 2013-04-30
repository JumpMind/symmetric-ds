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
package org.jumpmind.symmetric.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.file.FileTriggerTracker;
import org.jumpmind.symmetric.model.FileConflictStrategy;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;

public class FileSyncService extends AbstractService implements IFileSyncService {
    
    IClusterService clusterService;
    
    ITriggerRouterService triggerRouterService;
    
    private Map<FileTrigger, FileTriggerTracker> trackers = new HashMap<FileTrigger, FileTriggerTracker>();

    public FileSyncService(IParameterService parameterService, ISymmetricDialect symmetricDialect, IClusterService clusterService, ITriggerRouterService triggerRouterService) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
        this.triggerRouterService = triggerRouterService;
        setSqlMap(new FileSyncServiceSqlMap(platform, createSqlReplacementTokens()));
    }

    public void trackChanges() {
        List<FileTrigger> fileTriggers = getFileTriggersForCurrentNode();        
        for (FileTrigger fileTrigger : fileTriggers) {
            FileTriggerTracker tracker = trackers.get(fileTrigger);
            if (tracker == null) {
                tracker = new FileTriggerTracker(fileTrigger, getDirectorySnapshot(fileTrigger));
                trackers.put(fileTrigger, tracker);
            }
            try {
                save(tracker.trackChanges());
            } catch (Exception ex) {
                // TODO rollback snapshot if we fail to save to database
                log.error("Failed to track changes for file trigger: " + fileTrigger.getTriggerId(), ex);
            }
        }
    }

    public List<FileTrigger> getFileTriggers() {
        return null;
    }

    public FileTrigger getFileTrigger(String triggerId) {
        return null;
    }

    public void saveFileTrigger(FileTrigger fileTrigger) {
    }

    public void saveFileTriggerRouter(FileTriggerRouter fileTriggerRouter) {
    }

    public List<FileTriggerRouter> getFileTriggerRouters(FileTrigger fileTrigger) {
        return null;
    }
    
    public List<FileTrigger> getFileTriggersForCurrentNode() {
        return null;
    }    

    public List<FileTriggerRouter> getFileTriggerRoutersForCurrentNode() {
        return null;
    }

    public DirectorySnapshot getDirectorySnapshot(FileTrigger fileTrigger) {
        return null;
    }

    public void save(List<FileSnapshot> changes) {
    }
    
    class FileTriggerMapper implements ISqlRowMapper<FileTrigger> {
        public FileTrigger mapRow(Row rs) {
            FileTrigger fileTrigger = new FileTrigger();
            fileTrigger.setBaseDir(rs.getString("base_dir"));
            fileTrigger.setCreateTime(rs.getDateTime("create_time"));
            fileTrigger.setExcludesFiles(rs.getString("excludes_files"));
            fileTrigger.setIncludesFiles(rs.getString("includes_files"));
            fileTrigger.setLastUpdateBy(rs.getString("last_update_by"));
            fileTrigger.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileTrigger.setRecursive(rs.getBoolean("recursive"));
            fileTrigger.setSyncOnCreate(rs.getBoolean("sync_on_create"));
            fileTrigger.setSyncOnDelete(rs.getBoolean("sync_on_delete"));
            fileTrigger.setSyncOnModified(rs.getBoolean("sync_on_modified"));
            fileTrigger.setTriggerId("trigger_id");
            return fileTrigger;
        }
    }
    
    class FileTriggerRouterMapper implements ISqlRowMapper<FileTriggerRouter> {
        public FileTriggerRouter mapRow(Row rs) {
            FileTriggerRouter fileTriggerRouter = new FileTriggerRouter();
            String triggerId = rs.getString("trigger_id");
            FileTrigger fileTrigger = getFileTrigger(triggerId);
            fileTriggerRouter.setFileTrigger(fileTrigger);
            fileTriggerRouter.setConflictStrategy(FileConflictStrategy.valueOf(rs.getString("conflict_strategy").toUpperCase()));
            fileTriggerRouter.setCreateTime(rs.getDateTime("create_time"));
            fileTriggerRouter.setLastUpdateBy(rs.getString("last_update_by"));
            fileTriggerRouter.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileTriggerRouter.setEnabled(rs.getBoolean("enabled"));
            fileTriggerRouter.setInitialLoadEnabled(rs.getBoolean("initial_load_enabled"));
            fileTriggerRouter.setTargetBaseDir(rs.getString("target_base_dir"));
            fileTriggerRouter.setRouter(triggerRouterService.getRouterById(rs.getString("router_id")));
            return fileTriggerRouter;
        }
    }
    
    class FileSnapshotMapper implements ISqlRowMapper<FileSnapshot> {
        public FileSnapshot mapRow(Row rs) {
            FileSnapshot fileSnapshot = new FileSnapshot();
            fileSnapshot.setCrc32Checksum(rs.getLong("crc32_checksum"));
            fileSnapshot.setCreateTime(rs.getDateTime("create_time"));
            fileSnapshot.setLastUpdateBy(rs.getString("last_update_by"));
            fileSnapshot.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileSnapshot.setFileModifiedTime(rs.getDateTime("file_modified_time"));
            fileSnapshot.setFileName(rs.getString("file_name"));
            fileSnapshot.setFilePath(rs.getString("file_path"));
            fileSnapshot.setFileSize(rs.getLong("file_size"));
            fileSnapshot.setLastEventType(LastEventType.fromCode(rs.getString("last_event_type")));
            fileSnapshot.setTriggerId(rs.getString("trigger_id"));
            return fileSnapshot;
        }
    }

}
