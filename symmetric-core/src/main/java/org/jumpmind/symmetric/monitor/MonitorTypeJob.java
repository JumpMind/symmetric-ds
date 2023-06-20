package org.jumpmind.symmetric.monitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.MonitorEvent;
import org.jumpmind.symmetric.statistic.JobStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorTypeJob extends AbstractMonitorType implements IBuiltInExtensionPoint {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public String getName() {
        return "job";
    }

    @Override
    public MonitorEvent check(Monitor monitor) {
        MonitorEvent event = new MonitorEvent();
        List<JobStats> jobStatsList = engine.getStatisticService().getJobStatsForNode(engine.getNodeId());
        if (!jobStatsList.isEmpty()) {
            Map<String, Set<String>> errorMessagesByJobMap = new HashMap<String, Set<String>>();
            Set<String> jobsNotInErrorSet = new HashSet<String>();
            for (JobStats stats : jobStatsList) {
                String jobName = stats.getJobName();
                if (jobsNotInErrorSet.contains(jobName)) {
                    continue;
                }
                if (stats.isErrorFlag()) {
                    Set<String> errorMessageSet = errorMessagesByJobMap.get(jobName);
                    if (errorMessageSet == null) {
                        errorMessageSet = new HashSet<String>();
                    }
                    errorMessageSet.add(stats.getErrorMessage());
                    errorMessagesByJobMap.put(jobName, errorMessageSet);
                } else {
                    jobsNotInErrorSet.add(jobName);
                }
            }
            if (!errorMessagesByJobMap.isEmpty()) {
                event.setValue(errorMessagesByJobMap.size());
                String details = "";
                for (Entry<String, Set<String>> entry : errorMessagesByJobMap.entrySet()) {
                    Set<String> errorMessages = entry.getValue();
                    if (errorMessages.size() == 1) {
                        details += "1 unique error message for the " + entry.getKey() + " job: \""
                                + errorMessages.iterator().next() + "\", ";
                    } else {
                        details += errorMessages.size() + " unique error messages for the " + entry.getKey() + " job: ";
                        for (String errorMessage : errorMessages) {
                            details += "\"" + errorMessage + "\", ";
                        }
                    }
                }
                details = details.substring(0, details.length() - 2);
                event.setDetails(details);
            }
        }
        return event;
    }
}
