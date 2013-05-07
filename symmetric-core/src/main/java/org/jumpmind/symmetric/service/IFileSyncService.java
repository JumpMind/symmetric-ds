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
package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public interface IFileSyncService {

    public void trackChanges(boolean force);

    public List<FileTrigger> getFileTriggers();

    public FileTrigger getFileTrigger(String triggerId);

    public void saveFileTrigger(FileTrigger fileTrigger);

    public void saveFileTriggerRouter(FileTriggerRouter fileTriggerRouter);

    public List<FileTriggerRouter> getFileTriggerRouters(FileTrigger fileTrigger);

    public DirectorySnapshot getDirectorySnapshot(FileTriggerRouter fileTriggerRouter);

    public void save(List<FileSnapshot> changes);

    public List<FileTriggerRouter> getFileTriggerRoutersForCurrentNode();

    public FileTriggerRouter getFileTriggerRouterForCurrentNode(String triggerId, String routerId);   

    public RemoteNodeStatuses pullFilesFromNodes(boolean force);

    public RemoteNodeStatuses pushFilesToNodes(boolean force);
    
    public void sendFiles(ProcessInfo processInfo, Node node, IOutgoingTransport outgoingTransport);

}
