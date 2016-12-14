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

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.service.IOutgoingBatchService;

public class MonitorTypeBatchUnsent implements IMonitorType, ISymmetricEngineAware, IBuiltInExtensionPoint {

    protected IOutgoingBatchService outgoingBatchService;

    @Override
    public String getName() {
        return "batchUnsent";
    }

    @Override
    public MonitorEvent check(Monitor monitor) {
        MonitorEvent event = new MonitorEvent();
        event.setValue(outgoingBatchService.countOutgoingBatchesUnsent());
        return event;
    }

    @Override
    public boolean requiresClusterLock() {
        return true;
    }
    
    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        outgoingBatchService = engine.getOutgoingBatchService();
    }

}
