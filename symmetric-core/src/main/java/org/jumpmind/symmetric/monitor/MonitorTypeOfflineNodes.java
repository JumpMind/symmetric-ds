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
package org.jumpmind.symmetric.monitor;

import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MonitorTypeOfflineNodes implements IMonitorType, ISymmetricEngineAware, IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected INodeService nodeService;
    
    protected IParameterService parameterService;

    @Override
    public String getName() {
        return "offlineNodes";
    }

    @Override
    public MonitorEvent check(Monitor monitor) {
        int minutesBeforeNodeIsOffline = parameterService.getInt(
                ParameterConstants.MINUTES_BEFORE_NODE_REPORTED_AS_OFFLINE, 24 * 60);
        MonitorEvent event = new MonitorEvent();
        List<String> offlineNodes = nodeService.findOfflineNodeIds(minutesBeforeNodeIsOffline);
        event.setValue(offlineNodes.size());
        event.setDetails(serializeDetails(offlineNodes));
        return event;
    }

    @Override
    public boolean requiresClusterLock() {
        return true;
    }
    
    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        nodeService = engine.getNodeService();
        parameterService = engine.getParameterService();
    }
    
    protected String serializeDetails(List<String> offlineNodes) {
        ObjectMapper mapper = new ObjectMapper();
        
        String result = null;
        try {
            result = mapper.writeValueAsString(offlineNodes);
        } catch(JsonProcessingException jpe) {
            log.warn("Unable to convert list of offline nodes to JSON", jpe);
        }
       
        return result;
    }

}
