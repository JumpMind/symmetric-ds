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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.util.LogSummaryAppenderUtils;
import org.jumpmind.symmetric.util.SuperClassExclusion;
import org.jumpmind.util.LogSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.GsonBuilder;

public class MonitorTypeLog implements IMonitorType, ISymmetricEngineAware, IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    ISymmetricEngine engine;
    
    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }
    
    @Override
    public MonitorEvent check(Monitor monitor) {
        List<LogSummary> all = new ArrayList<LogSummary>();
        MonitorEvent event = new MonitorEvent();
        
        if (monitor.getSeverityLevel() == Monitor.SEVERE) {
            all.addAll(LogSummaryAppenderUtils.getLogSummaryErrors(engine.getEngineName()));
        } else if (monitor.getSeverityLevel() == Monitor.WARNING) {
            all.addAll(LogSummaryAppenderUtils.getLogSummaryWarnings(engine.getEngineName()));
        }
        
        Collections.sort(all);
        
        int count = 0;
        for (LogSummary logSummary : all) {
            count += logSummary.getCount();
        }

        event.setDetails(serializeDetails(all));
        event.setValue(all.size());
        event.setCount(count);
        
        return event;
    }

    protected String serializeDetails(List<LogSummary> logs) {
        String result = null;
        try {
            GsonBuilder builder = new GsonBuilder();
            builder.addSerializationExclusionStrategy(new SuperClassExclusion());
            builder.addDeserializationExclusionStrategy(new SuperClassExclusion());
            result = builder.create().toJson(logs);
        } catch(Exception e) {
            log.warn("Unable to convert list of logs to JSON", e);
        }
       
        return result;
    }
    
    @Override
    public boolean requiresClusterLock() {
        return false;
    }

    @Override
    public String getName() {
        return "log";
    }
}
