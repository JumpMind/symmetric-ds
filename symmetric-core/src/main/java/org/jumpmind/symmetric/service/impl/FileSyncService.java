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

import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.file.FileTriggerTracker;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IParameterService;

public class FileSyncService extends AbstractService implements IFileSyncService {
    
    IClusterService clusterService;
    
    private Map<FileTrigger, FileTriggerTracker> trackers = new HashMap<FileTrigger, FileTriggerTracker>();

    public FileSyncService(IParameterService parameterService, ISymmetricDialect symmetricDialect, IClusterService clusterService) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
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
                save(tracker.takeSnapshot());
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

}
