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
package org.jumpmind.symmetric.fs.config;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.fs.SyncParameterConstants;

public class SyncConfigCollection {

    protected TypedProperties properties;
    protected List<Node> serverNodes;
    protected List<SyncConfig> syncConfigs;

    public SyncConfigCollection() {
        properties = new TypedProperties();
        fillPropertyDefaults(properties);
        serverNodes = new ArrayList<Node>();
        syncConfigs = new ArrayList<SyncConfig>();
    }
    
    public static void fillPropertyDefaults(TypedProperties properties) {
        properties.setProperty(SyncParameterConstants.ENGINE_NAME, "symmetric");
        properties.setProperty(SyncParameterConstants.CLIENT_WORKER_THREADS_NUMBER, 10);
        properties.setProperty(SyncParameterConstants.JOB_RANDOM_MAX_START_TIME_MS, 10000);
    }
    
    public List<Node> getServerNodesForGroup(String groupId) {
        List<Node> nodes = new ArrayList<Node>();
        for (Node node : serverNodes) {
            if (node.getGroupId().equals(groupId)) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    public List<Node> getServerNodes() {
        return serverNodes;
    }
    
    public List<SyncConfig> getSyncConfigs() {
        return syncConfigs;
    }

    public TypedProperties getProperties() {
        return properties;
    }

}
