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
package org.jumpmind.symmetric.fs.client.connector;

import org.jumpmind.symmetric.fs.client.SyncStatus;
import org.jumpmind.symmetric.fs.config.SyncConfig;
import org.jumpmind.symmetric.fs.track.DirectorySpecSnapshot;

public class LocalTransportConnector extends AbstractTransportConnector implements
        ITransportConnector {

    public void connect() {
    }

    public void prepare(SyncStatus status) {
        SyncConfig syncConfig = status.getSyncConfig();
        
        //persisterSerivces.getDirectorySpecSnapshotPersister().get(DirectorySpecSnapshot.class, );
        DirectorySpecSnapshot snapshot = status.getDirectorySpecSnapshot();
        
    }

    public void send(SyncStatus syncStatus) {
    }

    public void receive(SyncStatus syncStatus) {
    }

}
