package org.jumpmind.symmetric.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.util.LogSummaryAppenderUtils;
import org.jumpmind.util.LogSummary;

public class MonitorTypeLog implements IMonitorType, ISymmetricEngineAware, IBuiltInExtensionPoint {

    ISymmetricEngine engine;
    
    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }
    
    @Override
    public long check(Monitor monitor) {
        List<LogSummary> all = new ArrayList<LogSummary>();
        if (monitor.getSeverityLevel() == Monitor.SEVERE) {
            all.addAll(LogSummaryAppenderUtils.getLogSummaryErrors(engine.getEngineName()));
        } else if (monitor.getSeverityLevel() == Monitor.WARNING) {
            all.addAll(LogSummaryAppenderUtils.getLogSummaryWarnings(engine.getEngineName()));
        }
        Collections.sort(all);

        LogSummaryAppenderUtils.clearAllLogSummaries(engine.getEngineName());
        return all.size();
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
