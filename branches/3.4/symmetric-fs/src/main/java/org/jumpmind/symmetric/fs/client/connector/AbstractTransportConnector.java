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

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.fs.client.SyncStatus;
import org.jumpmind.symmetric.fs.config.Node;
import org.jumpmind.symmetric.fs.service.IPersisterServices;
import org.jumpmind.symmetric.fs.track.DirectoryChangeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractTransportConnector implements ITransportConnector {

    final protected Logger log = LoggerFactory.getLogger(getClass());
    protected Node node;
    protected IPersisterServices persisterSerivces;
    protected TypedProperties properties;

    public void init(Node serverNode, IPersisterServices persisterServices, TypedProperties properties) {
        this.node = serverNode;
        this.persisterSerivces = persisterServices;
        this.properties = properties;
    }

    public void connect(SyncStatus syncStatus) {
    }

    public void prepare(SyncStatus syncStatus) {
    }

    public void send(SyncStatus syncStatus) {
    }

    public void receive(SyncStatus syncStatus, DirectoryChangeTracker clientDirectoryChangeTracker) {
    }

    public void close() {
    }
    
    public void destroy() {
    }

}
