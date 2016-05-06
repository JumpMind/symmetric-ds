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
package org.jumpmind.util;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

public class LogSummaryAppender extends AppenderSkeleton {

    Map<String, Map<String, LogSummary>> errorsByEngineByMessage = new ConcurrentHashMap<String, Map<String, LogSummary>>();

    Map<String, Map<String, LogSummary>> warningByEngineByMessage = new ConcurrentHashMap<String, Map<String, LogSummary>>();

    @Override
    protected void append(LoggingEvent event) {
        Map<String, Map<String, LogSummary>> summaries = null;
        if (event.getLevel() == Level.ERROR) {
            summaries = errorsByEngineByMessage;
        } else if (event.getLevel() == Level.WARN) {
            summaries = warningByEngineByMessage;
        }

        if (summaries != null) {
            String engineName = (String) event.getMDC("engineName");
            if (isNotBlank(engineName)) {
                Map<String, LogSummary> byMessage = summaries.get(engineName);
                if (byMessage == null) {
                    byMessage = new ConcurrentHashMap<String, LogSummary>();
                    summaries.put(engineName, byMessage);
                }

                String message = null;
                if (event.getMessage() != null) {
                    message = event.getMessage().toString();
                } else {
                    message = "No Message";
                }
                LogSummary summary = byMessage.get(message);
                if (summary == null) {
                    summary = new LogSummary();
                    summary.setMessage(message);
                    summary.setFirstOccurranceTime(event.getTimeStamp());
                    byMessage.put(message, summary);
                }
                summary.setLevel(event.getLevel());
                summary.setMostRecentTime(event.getTimeStamp());
                summary.setCount(summary.getCount() + 1);
                summary.setThrowable(event.getThrowableInformation() != null ? event
                        .getThrowableInformation().getThrowable() : null);
                summary.setMostRecentThreadName(event.getThreadName());
            }
        }
    }

    public void close() {
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public List<LogSummary> getLogSummaries(String engineName, Level level) {
        Map<String, Map<String, LogSummary>> summaries = null;
        if (level == Level.ERROR) {
            summaries = errorsByEngineByMessage;
        } else if (level == Level.WARN) {
            summaries = warningByEngineByMessage;
        }

        List<LogSummary> list = new ArrayList<LogSummary>();
        if (summaries != null && summaries.get(engineName) != null) {
            list.addAll(summaries.get(engineName).values());
            Collections.sort(list, new Comparator<LogSummary>() {
                @Override
                public int compare(LogSummary o1, LogSummary o2) {
                    return new Long(o1.getMostRecentTime()).compareTo(o2.getMostRecentTime());
                }
            });
        }
        return list;
    }
    
    public void clearAll(String engineName) {
        errorsByEngineByMessage.remove(engineName);
        warningByEngineByMessage.remove(engineName);
    }
    
    public void purgeOlderThan(long time) {
        purgeOlderThan(time, errorsByEngineByMessage);
        purgeOlderThan(time, warningByEngineByMessage);
    }
    
    protected void purgeOlderThan(long time,
            Map<String, Map<String, LogSummary>> logSummaryByEngineByMessage) {
        Collection<Map<String, LogSummary>> all = logSummaryByEngineByMessage.values();
        for (Map<String, LogSummary> map : all) {
            Set<String> keys = map.keySet();
            for (String key : keys) {
                LogSummary summary = map.get(key);
                if (summary != null) {
                    if (summary.getMostRecentTime() < time) {
                        map.remove(key);
                    }
                }
            }
        }
    }

}
