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

import java.util.Map;

public class SyncConfig {
    
    protected String configId;
    
    protected GroupLink groupLink;     
    
    protected String frequency;
    
    protected DirectorySpec directorySpec;
    
    protected String serverDir;
    
    protected ScriptType scriptType;
    
    protected int processOrder;
    
    protected String transportConnectorType = "default";
    
    protected String transportConnectorExpression;
    
    protected Map<ScriptIdentifier, String> scripts;
    
    public String getConfigId() {
        return configId;
    }
    
    public DirectorySpec getDirectorySpec() {
        return directorySpec;
    }
    
    public GroupLink getGroupLink() {
        return groupLink;
    }
    
    public String getFrequency() {
        return frequency;
    }
    
    public int getProcessOrder() {
        return processOrder;
    }
    
    public String getTransportConnectorType() {
        return transportConnectorType;
    }
    
    public String getTransportConnectorExpression() {
        return transportConnectorExpression;
    }

}
